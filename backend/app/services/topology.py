"""编排图拓扑预检引擎：纯计算，不读写数据仓库。

职责边界：
* ``merge_topology`` 把同一编排图的多次画布保存按「节点版本 / 边版本」合并，
  不是简单取最后一次保存的整张画布；
* ``analyze_topology`` 规范化依赖边后做环检测、孤立节点识别、分层与并行度计算；
* ``build_partition_dependencies`` 产出分区依赖数据，每条都能定位环、孤立节点和受影响对象；
* ``build_failure_propagation`` 产出失败传播清单，供发布预检与运行实例共用。

所有输入都是 ``store`` 里的普通 dict，函数不产生副作用，方便单独测试。
"""
from __future__ import annotations

import hashlib
import json
from typing import Any, Iterable

NODE_KEY = "节点编号"
UPSTREAM = "上游节点"
DOWNSTREAM = "下游节点"


def text(value: Any) -> str:
    """规范化文本：去掉首尾空白、把连续空白压成一个空格。"""
    return " ".join(str(value or "").split())


def number(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def node_signature(row: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        text(row.get("节点名称")),
        text(row.get("源端数据表")),
        text(row.get("目标数据表")),
        text(row.get("目标分区")),
    )


def edge_signature(row: dict[str, Any]) -> tuple[str, str, str, int]:
    return (
        text(row.get(UPSTREAM)),
        text(row.get(DOWNSTREAM)),
        text(row.get("依赖类型")) or "数据依赖",
        number(row.get("偏移量分钟")),
    )


def _save_rank(save: dict[str, Any]) -> tuple[int, int, int]:
    return (number(save.get("版本号")), number(save.get("id")), len(text(save.get("保存编号"))))


def topology_fingerprint(
    saves: Iterable[dict[str, Any]],
    nodes: Iterable[dict[str, Any]],
    edges: Iterable[dict[str, Any]],
) -> str:
    """保存指纹：预检之后只要画布再保存一次，指纹就会变，发布必须重新预检。"""
    payload = {
        "saves": sorted(
            (
                {
                    "保存编号": text(row.get("保存编号")),
                    "版本号": number(row.get("版本号")),
                    "节点数": number(row.get("节点数")),
                    "依赖边数": number(row.get("依赖边数")),
                }
                for row in saves
            ),
            key=lambda item: item["保存编号"],
        ),
        "nodes": sorted(
            (
                {
                    "保存编号": text(row.get("保存编号")),
                    "节点编号": text(row.get(NODE_KEY)),
                    "节点版本": number(row.get("节点版本"), 1),
                    "签名": node_signature(row),
                    "撤销": bool(row.get("撤销")),
                }
                for row in nodes
            ),
            key=lambda item: (item["保存编号"], item["节点编号"]),
        ),
        "edges": sorted(
            (
                {
                    "保存编号": text(row.get("保存编号")),
                    "上游节点": text(row.get(UPSTREAM)),
                    "下游节点": text(row.get(DOWNSTREAM)),
                    "边版本": number(row.get("边版本"), 1),
                    "签名": edge_signature(row),
                    "撤销": bool(row.get("撤销")),
                }
                for row in edges
            ),
            key=lambda item: (item["保存编号"], item["上游节点"], item["下游节点"]),
        ),
    }
    digest = hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()
    return digest[:16]


def merge_topology(
    save_rows: list[dict[str, Any]],
    node_rows: list[dict[str, Any]],
    edge_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """按节点/边版本合并多次保存，返回合并结果与版本决议。"""
    saves = sorted(save_rows, key=_save_rank)
    save_order = {
        text(save.get("保存编号")): index for index, save in enumerate(saves)
    }

    def candidate_order(row: dict[str, Any], version_field: str) -> tuple[int, int, int]:
        save_no = text(row.get("保存编号"))
        return (
            number(row.get(version_field), 1),
            save_order.get(save_no, -1),
            number(row.get("id")),
        )

    normalizations: list[str] = []
    decisions: list[dict[str, Any]] = []
    conflicts: list[dict[str, Any]] = []

    node_groups: dict[str, list[dict[str, Any]]] = {}
    for row in node_rows:
        raw_key = text(row.get(NODE_KEY))
        if not raw_key:
            continue
        if raw_key != str(row.get(NODE_KEY) or ""):
            normalizations.append(f"节点「{row.get(NODE_KEY)}」规范化为「{raw_key}」")
        node_groups.setdefault(raw_key, []).append(row)

    merged_nodes: dict[str, dict[str, Any]] = {}
    for key in sorted(node_groups):
        candidates = sorted(node_groups[key], key=lambda row: candidate_order(row, "节点版本"))
        chosen = candidates[-1]
        if bool(chosen.get("撤销")):
            decisions.append(
                {
                    "对象类型": "节点",
                    "对象编号": key,
                    "采用保存编号": text(chosen.get("保存编号")),
                    "采用版本": number(chosen.get("节点版本"), 1),
                    "结论": "较新保存已撤销该节点",
                    "候选": [],
                }
            )
            continue

        top_version = number(chosen.get("节点版本"), 1)
        same_version = [
            row for row in candidates if number(row.get("节点版本"), 1) == top_version
        ]
        signatures = {node_signature(row) for row in same_version}
        if len(signatures) > 1:
            conflicts.append(
                {
                    "对象类型": "节点",
                    "对象编号": key,
                    "版本": top_version,
                    "候选保存": [text(row.get("保存编号")) for row in same_version],
                    "采用保存编号": text(chosen.get("保存编号")),
                    "说明": "同一节点版本内容不一致，按保存版本号/保存编号取较新保存",
                }
            )

        if len(candidates) > 1 or top_version > 1:
            decisions.append(
                {
                    "对象类型": "节点",
                    "对象编号": key,
                    "采用保存编号": text(chosen.get("保存编号")),
                    "采用版本": top_version,
                    "结论": "按节点版本合并，较新版本覆盖较旧版本",
                    "候选": [
                        {
                            "保存编号": text(row.get("保存编号")),
                            "版本": number(row.get("节点版本"), 1),
                            "签名": list(node_signature(row)),
                        }
                        for row in candidates
                    ],
                }
            )

        merged_nodes[key] = {
            NODE_KEY: key,
            "节点名称": text(chosen.get("节点名称")) or key,
            "源端数据表": text(chosen.get("源端数据表")),
            "目标数据表": text(chosen.get("目标数据表")),
            "目标分区": text(chosen.get("目标分区")),
            "节点版本": top_version,
            "来源保存编号": text(chosen.get("保存编号")),
        }

    edge_groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in edge_rows:
        upstream = text(row.get(UPSTREAM))
        downstream = text(row.get(DOWNSTREAM))
        if not upstream or not downstream:
            continue
        if upstream != str(row.get(UPSTREAM) or "") or downstream != str(row.get(DOWNSTREAM) or ""):
            normalizations.append(
                f"依赖边「{row.get(UPSTREAM)} → {row.get(DOWNSTREAM)}」已去除首尾空白"
            )
        edge_groups.setdefault((upstream, downstream), []).append(row)

    merged_edges: list[dict[str, Any]] = []
    deduplicated = 0
    for (upstream, downstream), candidates in sorted(edge_groups.items()):
        candidates = sorted(candidates, key=lambda row: candidate_order(row, "边版本"))
        chosen = candidates[-1]
        if bool(chosen.get("撤销")):
            decisions.append(
                {
                    "对象类型": "依赖边",
                    "对象编号": f"{upstream} → {downstream}",
                    "采用保存编号": text(chosen.get("保存编号")),
                    "采用版本": number(chosen.get("边版本"), 1),
                    "结论": "较新保存已撤销该依赖边",
                    "候选": [],
                }
            )
            continue
        if len(candidates) > 1:
            deduplicated += len(candidates) - 1
            decisions.append(
                {
                    "对象类型": "依赖边",
                    "对象编号": f"{upstream} → {downstream}",
                    "采用保存编号": text(chosen.get("保存编号")),
                    "采用版本": number(chosen.get("边版本"), 1),
                    "结论": "多次保存的重复依赖边已合并",
                    "候选": [
                        {"保存编号": text(row.get("保存编号")), "版本": number(row.get("边版本"), 1)}
                        for row in candidates
                    ],
                }
            )
        merged_edges.append(
            {
                UPSTREAM: upstream,
                DOWNSTREAM: downstream,
                "边版本": number(chosen.get("边版本"), 1),
                "依赖类型": text(chosen.get("依赖类型")) or "数据依赖",
                "偏移量分钟": number(chosen.get("偏移量分钟")),
                "来源保存编号": text(chosen.get("保存编号")),
            }
        )

    if deduplicated:
        normalizations.append(f"合并重复依赖边 {deduplicated} 条")

    return {
        "节点清单": [merged_nodes[key] for key in sorted(merged_nodes)],
        "依赖边": merged_edges,
        "版本决议": decisions,
        "版本冲突": conflicts,
        "规范化记录": normalizations,
        "保存数量": len(saves),
    }


def _strong_components(
    keys: list[str], adjacency: dict[str, list[str]]
) -> list[list[str]]:
    """迭代版 Tarjan，避免大图递归过深。"""
    index: dict[str, int] = {}
    low: dict[str, int] = {}
    on_stack: set[str] = set()
    stack: list[str] = []
    components: list[list[str]] = []
    counter = 0

    for root in keys:
        if root in index:
            continue
        work: list[tuple[str, int]] = [(root, 0)]
        while work:
            node, child_index = work[-1]
            if child_index == 0 and node not in index:
                index[node] = low[node] = counter
                counter += 1
                stack.append(node)
                on_stack.add(node)
            if child_index < len(adjacency.get(node, [])):
                work[-1] = (node, child_index + 1)
                child = adjacency[node][child_index]
                if child not in index:
                    work.append((child, 0))
                elif child in on_stack:
                    low[node] = min(low[node], index[child])
            else:
                work.pop()
                if work:
                    parent = work[-1][0]
                    low[parent] = min(low[parent], low[node])
                if low[node] == index[node]:
                    component: list[str] = []
                    while True:
                        member = stack.pop()
                        on_stack.discard(member)
                        component.append(member)
                        if member == node:
                            break
                    components.append(sorted(component))
    return components


def _cycle_path(component: list[str], adjacency: dict[str, list[str]]) -> list[str]:
    allowed = set(component)
    start = component[0]
    if start in adjacency.get(start, []):
        return [start, start]
    stack: list[tuple[str, list[str]]] = [(start, [start])]
    seen: set[str] = set()
    while stack:
        node, path = stack.pop()
        for child in sorted(adjacency.get(node, []), reverse=True):
            if child not in allowed:
                continue
            if child == start:
                return path + [start]
            if child not in seen:
                seen.add(child)
                stack.append((child, path + [child]))
    return [start, start]


def analyze_topology(
    nodes: list[dict[str, Any]], edges: list[dict[str, Any]]
) -> dict[str, Any]:
    """对已合并的节点/依赖边做拓扑分析。"""
    keys = sorted(node["节点编号"] for node in nodes)
    known = set(keys)
    adjacency: dict[str, list[str]] = {key: [] for key in keys}
    reverse: dict[str, list[str]] = {key: [] for key in keys}
    incident: dict[str, int] = {key: 0 for key in keys}
    valid_edges: list[dict[str, Any]] = []
    dangling: list[dict[str, Any]] = []

    for edge in edges:
        upstream = edge[UPSTREAM]
        downstream = edge[DOWNSTREAM]
        if upstream not in known or downstream not in known:
            dangling.append(
                {
                    "上游节点": upstream,
                    "下游节点": downstream,
                    "缺失节点": [
                        item for item in (upstream, downstream) if item not in known
                    ],
                }
            )
            continue
        valid_edges.append(edge)
        adjacency[upstream].append(downstream)
        reverse[downstream].append(upstream)
        incident[upstream] += 1
        incident[downstream] += 1

    for key in keys:
        adjacency[key].sort()
        reverse[key].sort()

    components = _strong_components(keys, adjacency)
    cycles: list[dict[str, Any]] = []
    cycle_of: dict[str, str] = {}
    for component in components:
        has_self_loop = len(component) == 1 and component[0] in adjacency[component[0]]
        if len(component) == 1 and not has_self_loop:
            continue
        cycle_id = f"环-{len(cycles) + 1}"
        path = _cycle_path(component, adjacency)
        cycles.append(
            {
                "环编号": cycle_id,
                "路径": path,
                "节点": sorted(component),
                "长度": max(len(component) - 1, 1),
            }
        )
        for member in component:
            cycle_of[member] = cycle_id

    # 只有一个节点时不判孤立，单节点编排图没有依赖是合法形态。
    isolated = (
        [key for key in keys if incident[key] == 0] if len(keys) > 1 else []
    )

    # 孤立节点不参与分层：它没有可执行的上游/下游，不能因为入度为 0 就被排进首批。
    isolated_set = set(isolated)
    schedulable = [key for key in keys if key not in isolated_set]
    indegree = {key: 0 for key in schedulable}
    for edge in valid_edges:
        if edge[DOWNSTREAM] in indegree:
            indegree[edge[DOWNSTREAM]] += 1
    start_nodes = sorted(key for key in schedulable if indegree[key] == 0)
    queue = list(start_nodes)
    levels: dict[str, int] = {}
    remaining = dict(indegree)
    while queue:
        queue.sort()
        node = queue.pop(0)
        levels[node] = levels.get(node, 0)
        for child in adjacency[node]:
            if child not in remaining:
                continue
            remaining[child] -= 1
            if remaining[child] == 0:
                levels[child] = max(levels.get(child, 0), levels[node] + 1)
                queue.append(child)

    level_buckets: dict[int, list[str]] = {}
    for key, level in levels.items():
        level_buckets.setdefault(level, []).append(key)
    max_level = max(level_buckets) if level_buckets else 0
    max_parallelism = max((len(bucket) for bucket in level_buckets.values()), default=0)
    priorities: dict[str, int | None] = {}
    for key in keys:
        level = levels.get(key)
        priorities[key] = None if level is None else max_level - level + 1
    # 孤立节点没有层级，自然没有调度优先级；显式写出避免下游误判。

    reachable: dict[str, list[str]] = {}
    for key in keys:
        seen: set[str] = set()
        stack = list(adjacency[key])
        while stack:
            current = stack.pop()
            if current in seen:
                continue
            seen.add(current)
            stack.extend(adjacency[current])
        seen.discard(key)
        reachable[key] = sorted(seen)

    node_map = {node["节点编号"]: node for node in nodes}
    edge_flags = [
        {
            **edge,
            "是否成环": edge[UPSTREAM] in cycle_of
            and edge[DOWNSTREAM] in cycle_of
            and cycle_of[edge[UPSTREAM]] == cycle_of[edge[DOWNSTREAM]],
        }
        for edge in valid_edges
    ]

    return {
        "节点编号": keys,
        "节点": [node_map[key] for key in keys],
        "依赖边": edge_flags,
        "有效边": valid_edges,
        "悬空依赖": dangling,
        "环": cycles,
        "环节点": cycle_of,
        "孤立节点": isolated,
        "起始节点数": len(start_nodes),
        "层级": levels,
        "最大并行度": max_parallelism,
        "调度优先级": priorities,
        "下游可达": reachable,
        "最大层级": max_level,
    }


def affected_object(node: dict[str, Any]) -> str:
    table = text(node.get("目标数据表")) or "未声明数据表"
    partition = text(node.get("目标分区")) or "未声明分区"
    return f"{table}.{partition}"


def build_partition_dependencies(
    analysis: dict[str, Any],
) -> list[dict[str, Any]]:
    """一条记录对应一个目标数据表分区，可直接定位环 / 孤立节点 / 受影响对象。"""
    node_map = {node["节点编号"]: node for node in analysis["节点"]}
    level_map = analysis["层级"]
    priority_map = analysis["调度优先级"]
    cycle_map = analysis["环节点"]
    isolated = set(analysis["孤立节点"])
    incoming: dict[str, list[dict[str, Any]]] = {}
    for edge in analysis["有效边"]:
        incoming.setdefault(edge[DOWNSTREAM], []).append(edge)

    rows: list[dict[str, Any]] = []
    for node_key in analysis["节点编号"]:
        node = node_map[node_key]
        upstream_edges = sorted(
            incoming.get(node_key, []), key=lambda edge: edge[UPSTREAM]
        )
        downstream = analysis["下游可达"].get(node_key, [])
        affected_nodes = [node_key] + [item for item in downstream if item != node_key]
        affected_objects = [affected_object(node)]
        for item in downstream:
            if item == node_key:
                continue
            target = affected_object(node_map[item])
            if target not in affected_objects:
                affected_objects.append(target)
        rows.append(
            {
                "数据表": text(node.get("目标数据表")),
                "分区": text(node.get("目标分区")),
                "产出节点": node_key,
                "节点名称": node.get("节点名称", node_key),
                "层级": level_map.get(node_key),
                "调度优先级": priority_map.get(node_key),
                "上游节点": [edge[UPSTREAM] for edge in upstream_edges],
                "上游数据表": sorted(
                    {
                        text(node_map[edge[UPSTREAM]].get("目标数据表"))
                        for edge in upstream_edges
                    }
                ),
                "是否在环": node_key in cycle_map,
                "所属环": cycle_map.get(node_key),
                "是否孤立": node_key in isolated,
                "受影响节点": affected_nodes,
                "受影响数据表分区": affected_objects,
                "下游节点数": len([item for item in downstream if item != node_key]),
            }
        )
    return rows


def build_failure_propagation(
    analysis: dict[str, Any],
) -> list[dict[str, Any]]:
    """失败传播预演：每个节点失败后，下游节点与数据表分区会受什么影响。"""
    node_map = {node["节点编号"]: node for node in analysis["节点"]}
    priorities = analysis["调度优先级"]
    levels = analysis["层级"]
    propagation: list[dict[str, Any]] = []
    for node_key in analysis["节点编号"]:
        downstream = [
            item for item in analysis["下游可达"].get(node_key, []) if item != node_key
        ]
        objects: list[str] = []
        for item in downstream:
            target = affected_object(node_map[item])
            if target not in objects:
                objects.append(target)
        own_object = affected_object(node_map[node_key])
        propagation.append(
            {
                "触发节点": node_key,
                "节点名称": node_map[node_key].get("节点名称", node_key),
                "触发数据表": text(node_map[node_key].get("目标数据表")),
                "触发分区": text(node_map[node_key].get("目标分区")),
                "直接影响对象": own_object,
                "受影响节点": downstream,
                "受影响数据表分区": objects,
                "传播层级": max(
                    (levels[item] - levels[node_key] for item in downstream
                     if levels.get(item) is not None and levels.get(node_key) is not None),
                    default=0,
                ),
                "调度优先级": priorities.get(node_key),
                "传播说明": (
                    f"节点失败将阻断 {len(downstream)} 个下游节点、影响 {len(objects)} 个数据表分区"
                    if downstream
                    else "无下游节点，影响面仅本节点产出的数据表分区"
                ),
            }
        )
    return propagation


def assess(
    save_rows: list[dict[str, Any]],
    node_rows: list[dict[str, Any]],
    edge_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """完整预检计算：合并 → 分析 → 阻断判定 → 分区依赖 → 失败传播。"""
    merged = merge_topology(save_rows, node_rows, edge_rows)
    nodes = merged["节点清单"]
    edges = merged["依赖边"]
    analysis = analyze_topology(nodes, edges)

    blocking: list[str] = []
    if not save_rows or not nodes:
        blocking.append("编排图尚未保存任何拓扑节点，无法生成分区依赖数据")
    if not save_rows:
        blocking.append("缺少画布保存记录，无法确定节点版本来源")

    for item in analysis["悬空依赖"]:
        blocking.append(
            "依赖边引用了不存在的节点："
            f"{item['上游节点']} → {item['下游节点']}"
            f"（缺失：{'、'.join(item['缺失节点'])}）"
        )
    for node in nodes:
        if not text(node.get("目标数据表")):
            blocking.append(f"节点 {node['节点编号']} 缺少目标数据表，无法生成分区依赖数据")
        if not text(node.get("目标分区")):
            blocking.append(f"节点 {node['节点编号']} 缺少目标分区，无法生成分区依赖数据")
    for cycle in analysis["环"]:
        blocking.append(
            f"检测到{cycle['环编号']}：{' → '.join(cycle['路径'])}（涉及节点：{'、'.join(cycle['节点'])}）"
        )
    if analysis["孤立节点"]:
        blocking.append(
            "存在孤立节点（既无上游也无下游依赖）："
            + "、".join(analysis["孤立节点"])
        )
    for conflict in merged["版本冲突"]:
        blocking.append(
            f"节点 {conflict['对象编号']} 的版本 {conflict['版本']} 在多次保存中内容冲突，"
            f"已按保存顺序采用 {conflict['采用保存编号']}，请确认该版本是否符合预期"
        )

    partition_dependencies = build_partition_dependencies(analysis)
    propagation = build_failure_propagation(analysis)
    return {
        "合并结果": merged,
        "拓扑分析": analysis,
        "状态": "阻断" if blocking else "通过",
        "阻断原因": blocking,
        "分区依赖数据": partition_dependencies,
        "失败传播清单": propagation,
        "保存指纹": topology_fingerprint(save_rows, node_rows, edge_rows),
        "指标": {
            "节点数": len(nodes),
            "依赖边数": len(edges),
            "起始节点数": analysis["起始节点数"],
            "最大并行度": analysis["最大并行度"],
            "环数": len(analysis["环"]),
            "孤立节点数": len(analysis["孤立节点"]),
        },
    }
