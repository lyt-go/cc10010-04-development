"""运行实例业务规则：状态流转、失败传播清单生成与编排图快照联动。

运行实例不自己推导拓扑，而是读取编排图的生效分区依赖快照。这样发布预检的结论
会原样传到运行侧：预检阻断时给出阻断原因并允许重试，快照读取异常时留下可解释的
诊断信息，而不是返回一个空清单让人误以为“没有受影响对象”。
"""
from __future__ import annotations

from typing import Any

from app.services.dag import DagService, now
from app.services.topology import number
from app.store import store

MODULE = "runtime"
REQUIRED_FIELDS = ["记录编号", "作业名称", "开始时间"]
STATUS_ORDER = ["草稿", "运行中", "成功", "失败", "已终止"]
ACTION_RULES = {"启动作业": "运行中", "终止作业": "已终止", "标记成功": "成功"}
NEGATIVE_ACTIONS = []

RUN_PROPAGATION_MODULE = "runpropagation"


class RuntimeService:
    def __init__(self) -> None:
        self.dags = DagService()

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
            rows = [row for row in rows if keyword in str(row.get("记录编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return [self.decorate(row) for row in rows[start:start + size]], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        row = store.find(MODULE, entry_id)
        return self.decorate(row) if row else None

    def decorate(self, row: dict[str, Any]) -> dict[str, Any]:
        """列表里展示编排图联动状态，未关联编排图时明确说明而不是留空。"""
        enriched = dict(row)
        dag_id = number(row.get("编排图编号"))
        if not dag_id:
            enriched["编排图编号"] = ""
            enriched["失败传播状态"] = "未关联编排图"
            enriched["阻断原因"] = []
            return enriched
        view = self.propagation_view(int(row["id"]))
        enriched["失败传播状态"] = view.get("状态")
        enriched["阻断原因"] = view.get("阻断原因") or []
        enriched["生效快照"] = view.get("快照编号") or ""
        return enriched

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        dag_id = number(values.get("编排图编号"))
        if dag_id and store.find("dag", dag_id) is None:
            return None, [f"编排图 {dag_id} 不存在或已归档"]
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        entry["编排图编号"] = dag_id or ""
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"运行记录 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于运行实例可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"运行记录已{action}"

    # ---------------------------------------------------------------- 失败传播清单

    def latest_propagation(self, run_id: int) -> dict[str, Any] | None:
        rows = [
            row for row in store.rows(RUN_PROPAGATION_MODULE) if number(row.get("run_id")) == run_id
        ]
        if not rows:
            return None
        return max(rows, key=lambda row: number(row.get("id")))

    def list_propagations(self, run_id: int) -> list[dict[str, Any]]:
        return sorted(
            (
                dict(row)
                for row in store.rows(RUN_PROPAGATION_MODULE)
                if number(row.get("run_id")) == run_id
            ),
            key=lambda row: number(row.get("id")),
            reverse=True,
        )

    def propagation_view(self, run_id: int) -> dict[str, Any]:
        """读取运行实例的失败传播清单，状态实时跟随编排图预检结论。

        运行侧不缓存自己的结论：每次读取都以编排图当前的分区依赖状态为准，
        所以预检失败后清单立刻变成「阻断 + 原因」，重试预检通过后又能恢复。
        """
        run = store.find(MODULE, run_id)
        if run is None:
            return {
                "状态": "阻断",
                "阻断原因": [f"运行记录 {run_id} 不存在或已归档"],
                "失败传播清单": [],
                "分区依赖数据": [],
                "可重试": False,
            }
        dag_id = number(run.get("编排图编号"))
        if not dag_id:
            return {
                "状态": "未关联编排图",
                "阻断原因": ["运行记录未关联编排图，无法生成失败传播清单"],
                "失败传播清单": [],
                "分区依赖数据": [],
                "可重试": False,
            }

        dag = store.find("dag", dag_id)
        if dag is None:
            return {
                "状态": "阻断",
                "阻断原因": [f"关联的编排图 {dag_id} 不存在或已归档，失败传播清单不可用"],
                "失败传播清单": [],
                "分区依赖数据": [],
                "可重试": False,
                "编排图编号": dag_id,
            }

        partition_view = self.dags.partition_dependencies(dag_id)
        state = str(partition_view.get("状态") or "阻断")
        latest_run_propagation = self.latest_propagation(run_id)
        base = {
            "运行记录编号": run.get("记录编号"),
            "编排图编号": dag_id,
            "图编号": dag.get("图编号"),
            "预检状态": state,
            "预检编号": partition_view.get("预检编号", ""),
            "快照编号": partition_view.get("快照编号", ""),
        }

        if state != "生效":
            # 只有「旧快照仍可执行」时才允许读取上一份清单，且明确标注不是最新结论。
            keep_old = (
                partition_view.get("旧快照状态") == "仍可执行"
                and latest_run_propagation is not None
                and latest_run_propagation.get("状态") == "生效"
            )
            return {
                **base,
                "状态": state,
                "阻断原因": list(partition_view.get("阻断原因") or []),
                "说明": partition_view.get("说明") or "编排图拓扑未就绪，失败传播清单不可用",
                "失败传播清单": (
                    latest_run_propagation.get("失败传播清单") or [] if keep_old else []
                ),
                "分区依赖数据": (
                    latest_run_propagation.get("分区依赖数据") or [] if keep_old else []
                ),
                "可使用快照": partition_view.get("可使用快照", ""),
                "旧快照状态": partition_view.get("旧快照状态", ""),
                "清单来源": (
                    f"上一份生效清单（快照 {latest_run_propagation.get('快照编号')}，非最新拓扑）"
                    if keep_old
                    else ""
                ),
                "可重试": bool(partition_view.get("可重试", True)),
                "生成记录": latest_run_propagation,
            }

        if latest_run_propagation is None:
            return {
                **base,
                "状态": "未生成",
                "阻断原因": [],
                "失败传播清单": [],
                "分区依赖数据": [],
                "可重试": True,
                "说明": "快照已生效但运行实例尚未生成失败传播清单，请先生成",
            }

        if latest_run_propagation.get("快照编号") != partition_view.get("快照编号"):
            return {
                **base,
                "状态": "待刷新",
                "阻断原因": [],
                "失败传播清单": [],
                "分区依赖数据": [],
                "可重试": True,
                "说明": (
                    f"编排图生效快照已从 {latest_run_propagation.get('快照编号')} "
                    f"切换到 {partition_view.get('快照编号')}，请重新生成失败传播清单"
                ),
                "生成记录": latest_run_propagation,
            }

        return {
            **base,
            "状态": "生效",
            "阻断原因": [],
            "失败传播清单": partition_view.get("失败传播清单") or [],
            "分区依赖数据": partition_view.get("分区依赖数据") or [],
            "可重试": False,
            "清单来源": f"生效快照 {partition_view.get('快照编号')}",
            "说明": latest_run_propagation.get("说明") or "失败传播清单已按生效快照生成",
            "生成编号": latest_run_propagation.get("生成编号"),
            "生成时间": latest_run_propagation.get("生成时间"),
        }

    def generate_propagation(
        self, run_id: int, values: dict[str, Any] | None = None
    ) -> tuple[dict[str, Any] | None, str, dict[str, Any]]:
        """按编排图生效快照生成（或重试生成）失败传播清单。

        返回 (记录, 提示, 视图)：预检阻断 / 未就绪时记录为 None，视图里带齐原因，
        调用方可以直接展示原因并重试，不会拿到一个没有解释的空清单。
        """
        values = values or {}
        run = store.find(MODULE, run_id)
        if run is None:
            view = {
                "状态": "阻断",
                "阻断原因": [f"运行记录 {run_id} 不存在或已归档"],
                "失败传播清单": [],
                "可重试": False,
            }
            return None, "；".join(view["阻断原因"]), view

        dag_id = number(values.get("编排图编号")) or number(run.get("编排图编号"))
        if not dag_id:
            view = {
                "状态": "未关联编排图",
                "阻断原因": ["运行记录未关联编排图，无法生成失败传播清单"],
                "失败传播清单": [],
                "可重试": False,
            }
            return None, view["阻断原因"][0], view

        try:
            partition_view = self.dags.partition_dependencies(dag_id)
        except Exception as error:  # noqa: BLE001 - 跨模块读取中断必须可解释
            record = {
                "id": store.next_id(RUN_PROPAGATION_MODULE),
                "run_id": run_id,
                "dag_id": dag_id,
                "生成编号": "",
                "状态": "异常",
                "快照编号": "",
                "预检编号": "",
                "阻断原因": [f"读取编排图分区依赖快照失败：{error}"],
                "失败传播清单": [],
                "分区依赖数据": [],
                "重试次数": 0,
                "生成时间": now(),
                "说明": "跨模块读取中途异常，未写入空清单；排查后可重试生成",
            }
            record["生成编号"] = f"RUNP-{run_id:02d}-{record['id']:04d}"
            store.rows(RUN_PROPAGATION_MODULE).append(record)
            view = {
                "状态": "异常",
                "阻断原因": record["阻断原因"],
                "失败传播清单": [],
                "分区依赖数据": [],
                "可重试": True,
                "运行记录编号": run.get("记录编号"),
                "编排图编号": dag_id,
                "生成记录": record,
            }
            return None, record["阻断原因"][0], view
        state = partition_view.get("状态")
        retries = len(
            [
                row
                for row in store.rows(RUN_PROPAGATION_MODULE)
                if number(row.get("run_id")) == run_id
            ]
        )
        if state != "生效":
            reasons = list(partition_view.get("阻断原因") or [])
            if not reasons:
                reasons = [partition_view.get("说明") or f"编排图分区依赖状态为{state}，暂不能生成失败传播清单"]
            record = {
                "id": store.next_id(RUN_PROPAGATION_MODULE),
                "run_id": run_id,
                "dag_id": dag_id,
                "生成编号": "",
                "状态": state,
                "快照编号": partition_view.get("快照编号", ""),
                "预检编号": partition_view.get("预检编号", ""),
                "阻断原因": reasons,
                "失败传播清单": [],
                "分区依赖数据": [],
                "重试次数": retries,
                "生成时间": now(),
                "说明": "生成未通过，原因已记录；修正编排图后可重试",
            }
            record["生成编号"] = f"RUNP-{run_id:02d}-{record['id']:04d}"
            store.rows(RUN_PROPAGATION_MODULE).append(record)
            return None, "；".join(reasons), {**self.propagation_view(run_id), "生成记录": record}

        try:
            propagation = partition_view.get("失败传播清单") or []
            dependencies = partition_view.get("分区依赖数据") or []
            if not propagation:
                raise ValueError("生效快照里的失败传播清单为空，无法生成运行侧清单")
            record = {
                "id": store.next_id(RUN_PROPAGATION_MODULE),
                "run_id": run_id,
                "dag_id": dag_id,
                "生成编号": "",
                "状态": "生效",
                "快照编号": partition_view.get("快照编号"),
                "预检编号": partition_view.get("预检编号"),
                "阻断原因": [],
                "失败传播清单": propagation,
                "分区依赖数据": dependencies,
                "重试次数": retries,
                "生成时间": now(),
                "说明": "按编排图分区依赖快照生成的失败传播清单",
            }
            record["生成编号"] = f"RUNP-{run_id:02d}-{record['id']:04d}"
            store.rows(RUN_PROPAGATION_MODULE).append(record)
        except Exception as error:  # noqa: BLE001 - 异常也要留下可解释记录
            record = {
                "id": store.next_id(RUN_PROPAGATION_MODULE),
                "run_id": run_id,
                "dag_id": dag_id,
                "生成编号": "",
                "状态": "异常",
                "快照编号": partition_view.get("快照编号", ""),
                "预检编号": partition_view.get("预检编号", ""),
                "阻断原因": [f"失败传播清单生成异常：{error}"],
                "失败传播清单": [],
                "分区依赖数据": [],
                "重试次数": retries,
                "生成时间": now(),
                "说明": "生成过程中断，已保留诊断信息；可重试生成",
            }
            record["生成编号"] = f"RUNP-{run_id:02d}-{record['id']:04d}"
            store.rows(RUN_PROPAGATION_MODULE).append(record)
            return None, record["阻断原因"][0], {**self.propagation_view(run_id), "生成记录": record}

        try:
            final_view = self.propagation_view(run_id)
        except Exception as error:  # noqa: BLE001 - 清单已落库，读取失败不掩盖生成结果
            final_view = {
                "状态": "异常",
                "运行记录编号": run.get("记录编号"),
                "编排图编号": dag_id,
                "快照编号": record.get("快照编号"),
                "阻断原因": [f"清单已生成，但回读视图失败：{error}"],
                "说明": "生成结果已保存，可稍后重新读取",
                "失败传播清单": record.get("失败传播清单") or [],
                "分区依赖数据": record.get("分区依赖数据") or [],
                "可重试": True,
            }
            return record, "失败传播清单已生成，但回读视图异常", final_view
        return record, "失败传播清单已生成", final_view
