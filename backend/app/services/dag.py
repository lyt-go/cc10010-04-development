"""任务编排业务规则：画布保存、拓扑预检、事务化发布与快照读取。

关键约束：
* 同一编排图允许多次保存，节点 / 依赖边按版本合并，较晚保存的孤立节点同样进入
  预检输入，不会因为“只取最后一次画布”而丢失；
* 预检失败只记录阻断原因，不覆盖已有的可执行快照；只有校验通过且画布未再变更的
  预检才能发布；
* 发布动作在 ``store.transaction()`` 里同时写编排图状态、快照、失败传播清单和
  审计记录，并发发布时先拿到锁的一方生效，后到的一方被拒绝且旧快照保持生效；
* 读取侧永远返回可解释的状态：没有拓扑就说明缺少哪一步，预检异常也留下原因。
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from app.services.topology import (
    DOWNSTREAM,
    NODE_KEY,
    UPSTREAM,
    assess,
    merge_topology,
    number,
    text,
    topology_fingerprint,
)
from app.store import store

MODULE = "dag"
REQUIRED_FIELDS = ["图编号", "图名称", "拓扑结构"]
STATUS_ORDER = ["草稿", "编排评审中", "已发布", "已停用"]
ACTION_RULES = {"提交编排评审": "编排评审中", "发布编排": "已发布", "停用编排": "已停用"}
NEGATIVE_ACTIONS = ["停用编排"]

SAVE_MODULE = "dagsave"
NODE_MODULE = "dagnode"
EDGE_MODULE = "dagedge"
PRECHECK_MODULE = "dagprecheck"
SNAPSHOT_MODULE = "dagsnapshot"
PROPAGATION_MODULE = "dagpropagation"
AUDIT_MODULE = "topologyaudit"


def now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict):
            for key in ("节点列表", "nodes", "nodesList"):
                if isinstance(parsed.get(key), list):
                    return parsed[key]
    return []


def _normalize_save_payload(values: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    """把提交上来的画布整理成节点 / 依赖边，兼容 JSON 字符串的拓扑结构。"""
    raw_nodes = _as_list(values.get("节点列表"))
    raw_edges = _as_list(values.get("依赖边"))
    problems: list[str] = []

    topology = values.get("拓扑结构")
    if isinstance(topology, str) and topology.strip().startswith(("{", "[")):
        try:
            parsed = json.loads(topology)
        except json.JSONDecodeError:
            problems.append("拓扑结构不是合法 JSON，无法解析节点与依赖边")
            parsed = None
        if isinstance(parsed, dict):
            raw_nodes = raw_nodes or _as_list(parsed.get("节点列表") or parsed.get("nodes"))
            raw_edges = raw_edges or _as_list(parsed.get("依赖边") or parsed.get("edges"))
        elif isinstance(parsed, list):
            raw_nodes = raw_nodes or [
                item for item in parsed if isinstance(item, dict) and item.get(NODE_KEY)
            ]
            raw_edges = raw_edges or [
                item
                for item in parsed
                if isinstance(item, dict) and item.get(UPSTREAM) and item.get(DOWNSTREAM)
            ]

    if not raw_nodes:
        problems.append("保存内容缺少节点列表，至少需要一个拓扑节点")

    nodes: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, item in enumerate(raw_nodes, start=1):
        if not isinstance(item, dict):
            problems.append(f"第 {index} 个节点不是对象结构")
            continue
        key = text(item.get(NODE_KEY) or item.get("nodeId") or item.get("id"))
        if not key:
            problems.append(f"第 {index} 个节点缺少节点编号")
            continue
        if key in seen:
            problems.append(f"同一次保存里节点编号 {key} 重复，无法确定节点版本")
            continue
        seen.add(key)
        nodes.append(
            {
                NODE_KEY: key,
                "节点名称": text(item.get("节点名称") or item.get("name")) or key,
                "源端数据表": text(item.get("源端数据表") or item.get("sourceTable")),
                "目标数据表": text(item.get("目标数据表") or item.get("targetTable")),
                "目标分区": text(item.get("目标分区") or item.get("targetPartition")),
                "节点版本": max(number(item.get("节点版本"), 1), 1),
                "撤销": bool(item.get("撤销") or item.get("deleted")),
            }
        )

    edges: list[dict[str, Any]] = []
    edge_seen: set[tuple[str, str]] = set()
    for index, item in enumerate(raw_edges, start=1):
        if not isinstance(item, dict):
            problems.append(f"第 {index} 条依赖边不是对象结构")
            continue
        upstream = text(item.get(UPSTREAM) or item.get("source") or item.get("from"))
        downstream = text(item.get(DOWNSTREAM) or item.get("target") or item.get("to"))
        if not upstream or not downstream:
            problems.append(f"第 {index} 条依赖边缺少上游节点或下游节点")
            continue
        if (upstream, downstream) in edge_seen:
            problems.append(f"同一次保存里依赖边 {upstream} → {downstream} 重复")
            continue
        edge_seen.add((upstream, downstream))
        edges.append(
            {
                UPSTREAM: upstream,
                DOWNSTREAM: downstream,
                "依赖类型": text(item.get("依赖类型")) or "数据依赖",
                "偏移量分钟": number(item.get("偏移量分钟")),
                "边版本": max(number(item.get("边版本"), 1), 1),
                "撤销": bool(item.get("撤销") or item.get("deleted")),
            }
        )

    return nodes, edges, problems


class DagService:
    # ------------------------------------------------------------------ 列表 / 明细

    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("图编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return [self.decorate(row) for row in rows[start:start + size]], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        row = store.find(MODULE, entry_id)
        return self.decorate(row) if row else None

    def decorate(self, row: dict[str, Any]) -> dict[str, Any]:
        """列表里补上最近预检结论，避免把“未预检”误看成“通过”。"""
        dag_id = int(row.get("id", 0))
        latest_precheck = self.latest_precheck(dag_id)
        active_snapshot = self.active_snapshot(dag_id)
        enriched = dict(row)
        enriched["预检状态"] = (
            latest_precheck.get("状态") if latest_precheck else "未预检"
        )
        enriched["预检编号"] = latest_precheck.get("预检编号") if latest_precheck else ""
        enriched["阻断原因"] = list(latest_precheck.get("阻断原因") or []) if latest_precheck else []
        enriched["生效快照"] = active_snapshot.get("快照编号") if active_snapshot else ""
        enriched["拓扑版本"] = self.canvas_version(dag_id)
        return enriched

    def canvas_version(self, dag_id: int) -> int:
        row = store.find(MODULE, dag_id)
        return number(row.get("拓扑版本")) if row else 0

    # ------------------------------------------------------------------ 登记 / 动作

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        entry["拓扑版本"] = 0
        entry["发布版本"] = 0
        rows.append(entry)
        self.audit(entry["id"], "登记编排图", "成功", "已创建编排图，等待画布保存")
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"编排图 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于任务编排可执行范围"
        if action == "发布编排":
            return self.publish(entry_id, {})
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        self.audit(entry_id, action, "成功", f"编排图状态变更为{target}")
        return entry, f"编排图已{action}"

    # ------------------------------------------------------------------ 画布保存

    def save_canvas(
        self, dag_id: int, values: dict[str, Any]
    ) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, dag_id)
        if entry is None:
            return None, f"编排图 {dag_id} 不存在或已归档"
        if entry.get("status") == "已停用":
            return None, "编排图已停用，不能继续保存画布"

        nodes, edges, problems = _normalize_save_payload(values)
        if problems:
            self.audit(dag_id, "保存画布", "拒绝", "；".join(problems))
            return None, "；".join(problems)

        with store.transaction():
            version = max(
                (number(row.get("版本号")) for row in store.rows(SAVE_MODULE) if number(row.get("dag_id")) == dag_id),
                default=0,
            ) + 1
            save_no = text(values.get("保存编号")) or f"SAVE-{dag_id:02d}-{version:03d}"
            if any(
                text(row.get("保存编号")) == save_no and number(row.get("dag_id")) == dag_id
                for row in store.rows(SAVE_MODULE)
            ):
                return None, f"保存编号 {save_no} 已存在，请换一个保存编号"
            save_row = {
                "id": store.next_id(SAVE_MODULE),
                "dag_id": dag_id,
                "保存编号": save_no,
                "版本号": version,
                "画布说明": text(values.get("画布说明")) or f"第 {version} 次画布保存",
                "提交人": text(values.get("提交人")) or "值班管理员",
                "节点数": len(nodes),
                "依赖边数": len(edges),
                "保存时间": now(),
            }
            store.rows(SAVE_MODULE).append(save_row)

            for node in nodes:
                row = dict(node)
                row.update({"id": store.next_id(NODE_MODULE), "dag_id": dag_id, "保存编号": save_no})
                store.rows(NODE_MODULE).append(row)
            for edge in edges:
                row = dict(edge)
                row.update({"id": store.next_id(EDGE_MODULE), "dag_id": dag_id, "保存编号": save_no})
                store.rows(EDGE_MODULE).append(row)

            entry["拓扑版本"] = number(entry.get("拓扑版本")) + 1
            entry["拓扑结构"] = self.render_topology(nodes, edges)
            entry["起始节点数"] = ""
            entry["最大并行度"] = ""
            entry["pending"] = True
            entry["预检状态"] = "未预检"
            self.audit(
                dag_id,
                "保存画布",
                "成功",
                f"{save_no} 保存 {len(nodes)} 个节点、{len(edges)} 条依赖边，拓扑版本 {entry['拓扑版本']}",
            )
        return dict(save_row), f"画布已保存为 {save_no}，请执行拓扑预检"

    @staticmethod
    def render_topology(nodes: list[dict[str, Any]], edges: list[dict[str, Any]]) -> str:
        """列表里的拓扑结构列保持可读：优先展示依赖边，孤立节点单独标注。"""
        live_nodes = [node[NODE_KEY] for node in nodes if not node.get("撤销")]
        linked: set[str] = set()
        parts: list[str] = []
        for edge in edges:
            if edge.get("撤销"):
                continue
            parts.append(f"{edge[UPSTREAM]}→{edge[DOWNSTREAM]}")
            linked.update((edge[UPSTREAM], edge[DOWNSTREAM]))
        parts.extend(f"{key}(孤立)" for key in live_nodes if key not in linked)
        return "；".join(parts) if parts else "；".join(live_nodes)

    def list_saves(self, dag_id: int) -> list[dict[str, Any]]:
        return sorted(
            (
                dict(row)
                for row in store.rows(SAVE_MODULE)
                if number(row.get("dag_id")) == dag_id
            ),
            key=lambda row: (number(row.get("版本号")), number(row.get("id"))),
        )

    def _topology_inputs(self, dag_id: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        saves = self.list_saves(dag_id)
        nodes = [
            dict(row)
            for row in store.rows(NODE_MODULE)
            if number(row.get("dag_id")) == dag_id
        ]
        edges = [
            dict(row)
            for row in store.rows(EDGE_MODULE)
            if number(row.get("dag_id")) == dag_id
        ]
        return saves, nodes, edges

    # ------------------------------------------------------------------ 预检

    def audit(self, dag_id: int, action: str, result: str, detail: str) -> None:
        store.rows(AUDIT_MODULE).append(
            {
                "id": store.next_id(AUDIT_MODULE),
                "dag_id": dag_id,
                "动作": action,
                "结果": result,
                "说明": detail,
                "时间": now(),
            }
        )
        keep = 500
        rows = store.rows(AUDIT_MODULE)
        if len(rows) > keep:
            del rows[: len(rows) - keep]

    def precheck(self, dag_id: int, values: dict[str, Any] | None = None) -> dict[str, Any]:
        values = values or {}
        entry = store.find(MODULE, dag_id)
        if entry is None:
            return {
                "状态": "阻断",
                "阻断原因": [f"编排图 {dag_id} 不存在或已归档"],
                "分区依赖数据": [],
                "失败传播清单": [],
                "预检编号": "",
            }

        # 先落一条“运行中”预检：即使计算中途异常，读取侧也能解释结果去哪了。
        record: dict[str, Any] = {
            "id": store.next_id(PRECHECK_MODULE),
            "dag_id": dag_id,
            "预检编号": "",
            "状态": "运行中",
            "拓扑版本": self.canvas_version(dag_id),
            "保存指纹": "",
            "阻断原因": [],
            "版本决议": [],
            "版本冲突": [],
            "规范化记录": [],
            "指标": {},
            "分区依赖数据": [],
            "失败传播清单": [],
            "预检时间": now(),
            "重试次数": 0,
            "说明": "",
        }
        store.rows(PRECHECK_MODULE).append(record)
        record["预检编号"] = f"PRE-{dag_id:02d}-{record['id']:04d}"

        try:
            saves, nodes, edges = self._topology_inputs(dag_id)
            previous = [
                row
                for row in store.rows(PRECHECK_MODULE)
                if number(row.get("dag_id")) == dag_id and row is not record and row.get("阻断原因")
            ]
            record["重试次数"] = len(previous)
            result = assess(saves, nodes, edges)
            record.update(
                {
                    "状态": result["状态"],
                    "保存指纹": result["保存指纹"],
                    "阻断原因": result["阻断原因"],
                    "版本决议": result["合并结果"]["版本决议"],
                    "版本冲突": result["合并结果"]["版本冲突"],
                    "规范化记录": result["合并结果"]["规范化记录"],
                    "指标": result["指标"],
                    "分区依赖数据": result["分区依赖数据"],
                    "失败传播清单": result["失败传播清单"],
                    "说明": (
                        "拓扑预检通过，可执行发布"
                        if result["状态"] == "通过"
                        else f"拓扑预检阻断，共 {len(result['阻断原因'])} 条原因；修正画布后可重试"
                    ),
                }
            )
            if result["状态"] == "通过":
                entry["预检状态"] = "通过"
                entry["abnormal"] = False
                if "编排评审中" in STATUS_ORDER and entry.get("status") == "草稿":
                    entry["status"] = "编排评审中"
                    entry["pending"] = True
            else:
                entry["预检状态"] = "阻断"
                entry["abnormal"] = True
            self.audit(
                dag_id,
                "拓扑预检",
                result["状态"],
                record["说明"] + ("；" + "；".join(record["阻断原因"]) if record["阻断原因"] else ""),
            )
        except Exception as error:  # noqa: BLE001 - 预检异常必须留下可解释结论
            record.update(
                {
                    "状态": "异常",
                    "阻断原因": [f"拓扑预检执行异常：{error}"],
                    "说明": "预检未能完成，已保留诊断信息；请重试预检，原可执行快照未受影响",
                }
            )
            entry["预检状态"] = "异常"
            entry["abnormal"] = True
            self.audit(dag_id, "拓扑预检", "异常", record["说明"])
        return record

    def latest_precheck(self, dag_id: int) -> dict[str, Any] | None:
        rows = [
            row for row in store.rows(PRECHECK_MODULE) if number(row.get("dag_id")) == dag_id
        ]
        if not rows:
            return None
        return max(rows, key=lambda row: number(row.get("id")))

    def list_prechecks(self, dag_id: int) -> list[dict[str, Any]]:
        return sorted(
            (
                dict(row)
                for row in store.rows(PRECHECK_MODULE)
                if number(row.get("dag_id")) == dag_id
            ),
            key=lambda row: number(row.get("id")),
            reverse=True,
        )

    def get_precheck(self, dag_id: int, precheck_id: int) -> dict[str, Any] | None:
        for row in store.rows(PRECHECK_MODULE):
            if number(row.get("dag_id")) == dag_id and number(row.get("id")) == precheck_id:
                return row
        return None

    # ------------------------------------------------------------------ 发布 / 快照

    def active_snapshot(self, dag_id: int) -> dict[str, Any] | None:
        rows = [
            row
            for row in store.rows(SNAPSHOT_MODULE)
            if number(row.get("dag_id")) == dag_id and row.get("状态") == "生效"
        ]
        if not rows:
            return None
        return max(rows, key=lambda row: number(row.get("id")))

    def list_snapshots(self, dag_id: int) -> list[dict[str, Any]]:
        return sorted(
            (
                dict(row)
                for row in store.rows(SNAPSHOT_MODULE)
                if number(row.get("dag_id")) == dag_id
            ),
            key=lambda row: number(row.get("id")),
            reverse=True,
        )

    def publish(
        self, dag_id: int, values: dict[str, Any]
    ) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, dag_id)
        if entry is None:
            return None, f"编排图 {dag_id} 不存在或已归档"
        if entry.get("status") == "已停用":
            return None, "编排图已停用，不能发布拓扑快照"

        precheck_id = number(values.get("预检id"))
        precheck: dict[str, Any] | None = None
        if precheck_id:
            precheck = self.get_precheck(dag_id, precheck_id)
            if precheck is None:
                return None, f"预检记录 {precheck_id} 不属于编排图 {dag_id}，不能发布"
        else:
            latest = self.latest_precheck(dag_id)
            if latest is None:
                return None, "没有可用的预检记录，请先执行拓扑预检"
            if latest.get("状态") != "通过":
                reasons = "；".join(latest.get("阻断原因") or [])
                return None, (
                    f"最近一次预检状态为{latest.get('状态')}，不能发布"
                    + (f"：{reasons}" if reasons else "")
                )
            precheck = latest
        if precheck.get("状态") != "通过":
            reasons = "；".join(precheck.get("阻断原因") or [])
            return None, f"最近一次预检状态为{precheck.get('状态')}，不能发布" + (
                f"：{reasons}" if reasons else ""
            )

        with store.transaction():
            # 事务内重新校验画布：预检之后只要又保存过一次，旧结论就不能发布。
            if number(precheck.get("拓扑版本")) != self.canvas_version(dag_id):
                return None, "预检后画布又被保存过，拓扑版本已变化，请重新执行拓扑预检"
            if text(precheck.get("保存指纹")) != text(self._fingerprint(dag_id)):
                return None, "预检后画布内容已变化，保存指纹不一致，请重新执行拓扑预检"

            active = self.active_snapshot(dag_id)
            superseded: list[str] = []
            if active is not None:
                if number(active.get("预检id")) == number(precheck.get("id")):
                    return active, f"该预检对应的拓扑快照 {active.get('快照编号')} 已生效，无需重复发布"
                if text(active.get("保存指纹")) == text(precheck.get("保存指纹")):
                    return None, (
                        f"编排图已有同拓扑结构的生效快照 {active.get('快照编号')}，"
                        f"并发发布的预检 {precheck.get('预检编号')} 未生效；旧快照保持可执行"
                    )
                # 新拓扑替换旧拓扑：旧快照标记为已替换，保证同一时刻只有一份「生效」。
                for row in store.rows(SNAPSHOT_MODULE):
                    if number(row.get("dag_id")) == dag_id and row.get("状态") == "生效":
                        row["状态"] = "已替换"
                        row["替换时间"] = now()
                        superseded.append(row.get("快照编号"))
                for row in store.rows(PROPAGATION_MODULE):
                    if number(row.get("dag_id")) == dag_id and row.get("状态") == "生效":
                        row["状态"] = "已替换"

            snapshot_id = store.next_id(SNAPSHOT_MODULE)
            snapshot_no = f"SNAP-{dag_id:02d}-{snapshot_id:04d}"
            priority = precheck.get("分区依赖数据") or []
            snapshot = {
                "id": snapshot_id,
                "dag_id": dag_id,
                "快照编号": snapshot_no,
                "预检id": number(precheck.get("id")),
                "预检编号": precheck.get("预检编号"),
                "拓扑版本": number(precheck.get("拓扑版本")),
                "保存指纹": precheck.get("保存指纹"),
                "状态": "生效",
                "发布人": text(values.get("发布人")) or "值班管理员",
                "发布时间": now(),
                "节点结构": [
                    {
                        "节点编号": item.get("产出节点"),
                        "节点名称": item.get("节点名称"),
                        "目标数据表": item.get("数据表"),
                        "目标分区": item.get("分区"),
                        "层级": item.get("层级"),
                        "调度优先级": item.get("调度优先级"),
                        "上游节点": item.get("上游节点"),
                    }
                    for item in priority
                ],
                "指标": precheck.get("指标") or {},
                "说明": text(values.get("发布说明")) or "拓扑预检通过后发布的可执行快照",
            }
            store.rows(SNAPSHOT_MODULE).append(snapshot)

            store.rows(PROPAGATION_MODULE).append(
                {
                    "id": store.next_id(PROPAGATION_MODULE),
                    "dag_id": dag_id,
                    "快照id": snapshot_id,
                    "快照编号": snapshot_no,
                    "预检id": number(precheck.get("id")),
                    "预检编号": precheck.get("预检编号"),
                    "状态": "生效",
                    "失败传播清单": precheck.get("失败传播清单") or [],
                    "分区依赖数据": precheck.get("分区依赖数据") or [],
                    "生成时间": now(),
                    "说明": "发布事务内同步生成的失败传播清单",
                }
            )

            entry["status"] = "已发布"
            entry["pending"] = False
            entry["abnormal"] = False
            entry["预检状态"] = "通过"
            entry["发布版本"] = number(entry.get("发布版本")) + 1
            entry["生效快照"] = snapshot_no
            entry["起始节点数"] = (precheck.get("指标") or {}).get("起始节点数", "")
            entry["最大并行度"] = (precheck.get("指标") or {}).get("最大并行度", "")
            entry["失败传播策略"] = "按分区依赖快照逐层阻断"
            entry["调度优先级"] = "按拓扑层级倒序"
            entry["生效时间范围"] = entry.get("生效时间范围") or now()[:10]
            self.audit(
                dag_id,
                "发布编排",
                "成功",
                f"快照 {snapshot_no} 生效，预检 {precheck.get('预检编号')}，"
                f"失败传播清单 {len(precheck.get('失败传播清单') or [])} 条"
                + (f"；旧快照 {'、'.join(superseded)} 已替换" if superseded else ""),
            )
        return snapshot, (
            f"拓扑快照 {snapshot_no} 已生效，编排图状态与失败传播清单同时可见"
            + (f"；旧快照 {'、'.join(superseded)} 标记为已替换" if superseded else "")
        )

    def _fingerprint(self, dag_id: int) -> str:
        saves, nodes, edges = self._topology_inputs(dag_id)
        return topology_fingerprint(saves, nodes, edges)

    # ------------------------------------------------------------------ 预检结果 / 分区数据 / 传播清单

    def partition_dependencies(self, dag_id: int) -> dict[str, Any]:
        """分区依赖数据视图：状态跟随最近预检结论，同时说明旧快照是否仍可执行。

        判定顺序：
        1. 最近预检阻断 / 异常 → 状态跟随预检，返回阻断原因；旧生效快照保持可执行，
           但不再对外提供传播清单，避免把过期结论当成本次结果；
        2. 预检后画布又被保存 → 结论过期，状态「待重检」；
        3. 生效快照与最近通过预检一致 → 「生效」，返回分区依赖数据与失败传播清单；
        4. 预检通过但未发布 → 「待发布」；从未预检 → 「未预检」，都带可读说明。
        """
        entry = store.find(MODULE, dag_id)
        if entry is None:
            return {
                "状态": "阻断",
                "来源": "编排图不存在",
                "阻断原因": [f"编排图 {dag_id} 不存在或已归档"],
                "分区依赖数据": [],
                "失败传播清单": [],
            }

        active = self.active_snapshot(dag_id)
        latest = self.latest_precheck(dag_id)
        canvas_version = self.canvas_version(dag_id)
        fallback = self._snapshot_view(dag_id, active) if active else None

        if latest is None:
            saves = self.list_saves(dag_id)
            view = {
                "状态": "未预检",
                "来源": "尚未预检",
                "阻断原因": (
                    ["编排图尚未保存画布，无法生成分区依赖数据"]
                    if not saves
                    else [f"已保存 {len(saves)} 次画布但尚未执行拓扑预检，请先预检"]
                ),
                "分区依赖数据": [],
                "失败传播清单": [],
                "说明": "没有预检结论就没有分区依赖数据；执行拓扑预检后可查看",
                "拓扑版本": canvas_version,
                "可重试": True,
            }
            if fallback:
                view.update(
                    {
                        "可使用快照": fallback.get("快照编号"),
                        "说明": "画布有新的保存但尚未预检；旧生效快照仍可执行，但不代表最新拓扑",
                    }
                )
            return view

        # 「运行中」只剩在预检中途进程被强杀的情况：不能当成未预检，也不能当成通过。
        if latest.get("状态") == "运行中":
            view = {
                "状态": "预检中断",
                "来源": f"预检 {latest.get('预检编号')} 未完成",
                "预检编号": latest.get("预检编号"),
                "阻断原因": [
                    "上一次拓扑预检在完成前中断（进程异常退出），结论不可用；请重新执行拓扑预检"
                ],
                "说明": "预检中断不会生成或覆盖任何快照；旧生效快照保持可执行",
                "分区依赖数据": [],
                "失败传播清单": [],
                "拓扑版本": canvas_version,
                "可重试": True,
            }
            if fallback:
                view.update(
                    {
                        "可使用快照": fallback.get("快照编号"),
                        "旧快照状态": "仍可执行",
                    }
                )
            return view

        if latest.get("状态") in ("阻断", "异常"):
            view = {
                "状态": latest.get("状态"),
                "来源": f"最近预检 {latest.get('预检编号')}",
                "预检编号": latest.get("预检编号"),
                "阻断原因": latest.get("阻断原因") or [],
                "说明": latest.get("说明") or "预检未通过，失败传播清单不可用",
                "指标": latest.get("指标") or {},
                "分区依赖数据": [],
                "失败传播清单": [],
                "拓扑版本": latest.get("拓扑版本"),
                "可重试": True,
            }
            if fallback:
                view.update(
                    {
                        "可使用快照": fallback.get("快照编号"),
                        "旧快照状态": "仍可执行",
                        "说明": (
                            f"最近预检{latest.get('状态')}，本次拓扑不能发布；"
                            f"旧生效快照 {fallback.get('快照编号')} 保持可执行，但不含本次变更"
                        ),
                    }
                )
            return view

        if number(latest.get("拓扑版本")) != canvas_version or text(
            latest.get("保存指纹")
        ) != text(self._fingerprint(dag_id)):
            view = {
                "状态": "待重检",
                "来源": f"最近预检 {latest.get('预检编号')} 已过期",
                "预检编号": latest.get("预检编号"),
                "阻断原因": [
                    "预检之后画布又被保存，节点版本可能已变化，预检结论不再代表当前拓扑"
                ],
                "说明": "请重新执行拓扑预检；旧生效快照与新画布都未被覆盖",
                "分区依赖数据": [],
                "失败传播清单": [],
                "拓扑版本": canvas_version,
                "预检拓扑版本": latest.get("拓扑版本"),
                "可重试": True,
            }
            if fallback:
                view.update(
                    {
                        "可使用快照": fallback.get("快照编号"),
                        "旧快照状态": "仍可执行",
                    }
                )
            return view

        if fallback and number(active.get("预检id")) == number(latest.get("id")):
            view = dict(fallback)
            view.update(
                {
                    "状态": "生效",
                    "来源": f"生效快照 {active.get('快照编号')}",
                    "预检编号": latest.get("预检编号"),
                    "阻断原因": [],
                    "可重试": False,
                }
            )
            return view

        return {
            "状态": "待发布",
            "来源": f"最近预检 {latest.get('预检编号')} 已通过",
            "预检编号": latest.get("预检编号"),
            "阻断原因": [],
            "说明": "预检已通过但尚未发布生效快照，发布后运行侧才能生成失败传播清单",
            "指标": latest.get("指标") or {},
            "分区依赖数据": latest.get("分区依赖数据") or [],
            "失败传播清单": latest.get("失败传播清单") or [],
            "拓扑版本": latest.get("拓扑版本"),
            "可重试": True,
        }

    def _snapshot_view(self, dag_id: int, active: dict[str, Any]) -> dict[str, Any]:
        propagation = self._snapshot_propagation(dag_id, number(active.get("id")))
        return {
            "状态": "生效",
            "来源": f"生效快照 {active.get('快照编号')}",
            "快照编号": active.get("快照编号"),
            "预检编号": active.get("预检编号"),
            "阻断原因": [],
            "指标": active.get("指标") or {},
            "分区依赖数据": (propagation or {}).get("分区依赖数据") or [],
            "失败传播清单": (propagation or {}).get("失败传播清单") or [],
        }

    def _snapshot_propagation(self, dag_id: int, snapshot_id: int) -> dict[str, Any] | None:
        for row in store.rows(PROPAGATION_MODULE):
            if number(row.get("dag_id")) == dag_id and number(row.get("快照id")) == snapshot_id:
                return row
        return None

    def merge_preview(self, dag_id: int) -> dict[str, Any]:
        saves, nodes, edges = self._topology_inputs(dag_id)
        merged = merge_topology(saves, nodes, edges)
        return {
            "dag_id": dag_id,
            "保存数量": merged["保存数量"],
            "节点清单": merged["节点清单"],
            "依赖边": merged["依赖边"],
            "版本决议": merged["版本决议"],
            "版本冲突": merged["版本冲突"],
            "规范化记录": merged["规范化记录"],
        }

    def ensure_demo_publish(self) -> None:
        """启动时补齐示例数据，保证一打开就有「通过 / 阻断」两种可解释结果。"""
        for entry in store.rows(MODULE):
            dag_id = int(entry["id"])
            status = entry.get("status")
            if status == "已发布" and self.active_snapshot(dag_id) is None:
                record = self.precheck(dag_id)
                if record.get("状态") == "通过":
                    self.publish(dag_id, {"发布说明": "示例数据初始发布"})
            elif status == "编排评审中" and self.latest_precheck(dag_id) is None:
                # 示例编排图自带环、孤立节点和版本冲突，启动即落一条阻断预检。
                self.precheck(dag_id)
