"""拓扑发布预检的核心行为测试（标准库 unittest，直接跑服务层）。

运行：cd backend && python -m unittest discover -s tests -v
"""
from __future__ import annotations

import unittest
from concurrent.futures import ThreadPoolExecutor

from app.services.dag import DagService
from app.services.runtime import RuntimeService
from app.services.topology import analyze_topology, merge_topology
from app.store import store


def save_row(dag_id: int, no: str, version: int) -> dict:
    return {"id": version, "dag_id": dag_id, "保存编号": no, "版本号": version}


def node(dag_id: int, no: str, key: str, *, version: int = 1, table: str = "", part: str = "dt",
         revoked: bool = False) -> dict:
    return {
        "id": hash((no, key, version)) % 100000,
        "dag_id": dag_id,
        "保存编号": no,
        "节点编号": key,
        "节点名称": key,
        "源端数据表": f"ods.{key.lower()}",
        "目标数据表": table or f"dwd.{key.lower()}",
        "目标分区": part,
        "节点版本": version,
        "撤销": revoked,
    }


def edge(dag_id: int, no: str, up: str, down: str, *, version: int = 1, revoked: bool = False) -> dict:
    return {
        "id": hash((no, up, down)) % 100000,
        "dag_id": dag_id,
        "保存编号": no,
        "上游节点": up,
        "下游节点": down,
        "依赖类型": "数据依赖",
        "偏移量分钟": 0,
        "边版本": version,
        "撤销": revoked,
    }


class TopologyMergeTest(unittest.TestCase):
    def test_merge_keeps_older_isolated_node(self) -> None:
        saves = [save_row(1, "S1", 1), save_row(1, "S2", 2)]
        nodes = [
            node(1, "S1", "A"),
            node(1, "S1", "B"),
            node(1, "S2", "C", version=1),
        ]
        edges = [edge(1, "S1", "A", "B", version=1)]
        merged = merge_topology(saves, nodes, edges)
        keys = [item["节点编号"] for item in merged["节点清单"]]
        # 较晚保存的孤立节点 C 必须进入合并结果，而不是被最后一次画布覆盖掉。
        self.assertEqual(keys, ["A", "B", "C"])
        analysis = analyze_topology(merged["节点清单"], merged["依赖边"])
        self.assertEqual(analysis["孤立节点"], ["C"])
        self.assertEqual(analysis["最大并行度"], 1)

    def test_newer_node_version_overrides_older(self) -> None:
        saves = [save_row(1, "S1", 1), save_row(1, "S2", 2)]
        nodes = [
            node(1, "S1", "A", version=1, table="dwd.a_v1"),
            node(1, "S2", "A", version=2, table="dwd.a_v2"),
        ]
        merged = merge_topology(saves, nodes, [])
        self.assertEqual(merged["节点清单"][0]["目标数据表"], "dwd.a_v2")
        self.assertFalse(merged["版本冲突"])
        self.assertTrue(any(item["对象编号"] == "A" for item in merged["版本决议"]))

    def test_conflicting_same_version_is_reported(self) -> None:
        saves = [save_row(1, "S1", 1), save_row(1, "S2", 2)]
        nodes = [
            node(1, "S1", "A", version=3, table="dwd.a_left"),
            node(1, "S2", "A", version=3, table="dwd.a_right"),
        ]
        merged = merge_topology(saves, nodes, [])
        self.assertEqual(len(merged["版本冲突"]), 1)
        # 同一版本冲突时明确采用较晚保存，并在决议里保留两个候选。
        self.assertEqual(merged["节点清单"][0]["目标数据表"], "dwd.a_right")

    def test_revoked_node_removed(self) -> None:
        saves = [save_row(1, "S1", 1), save_row(1, "S2", 2)]
        nodes = [node(1, "S1", "A"), node(1, "S2", "A", version=2, revoked=True)]
        merged = merge_topology(saves, nodes, [])
        self.assertEqual(merged["节点清单"], [])

    def test_duplicate_edges_collapsed(self) -> None:
        saves = [save_row(1, "S1", 1), save_row(1, "S2", 2)]
        nodes = [node(1, "S1", "A"), node(1, "S1", "B")]
        edges = [edge(1, "S1", "A", "B"), edge(1, "S2", "A", "B", version=2)]
        merged = merge_topology(saves, nodes, edges)
        self.assertEqual(len(merged["依赖边"]), 1)
        self.assertTrue(any("合并重复依赖边" in item for item in merged["规范化记录"]))

    def test_whitespace_normalized(self) -> None:
        saves = [save_row(1, "S1", 1)]
        nodes = [node(1, "S1", " A "), node(1, "S1", "B")]
        edges = [edge(1, "S1", " A ", "B ")]
        merged = merge_topology(saves, nodes, edges)
        self.assertEqual([item["节点编号"] for item in merged["节点清单"]], ["A", "B"])
        self.assertEqual(merged["依赖边"][0]["上游节点"], "A")


class TopologyAnalysisTest(unittest.TestCase):
    def test_cycle_and_isolated_detection(self) -> None:
        nodes = [{"节点编号": key, "节点名称": key, "源端数据表": "", "目标数据表": f"t_{key}", "目标分区": "dt"}
                 for key in ("A", "B", "C", "Z")]
        edges = [
            {"上游节点": "A", "下游节点": "B", "依赖类型": "数据依赖", "偏移量分钟": 0},
            {"上游节点": "B", "下游节点": "C", "依赖类型": "数据依赖", "偏移量分钟": 0},
            {"上游节点": "C", "下游节点": "A", "依赖类型": "数据依赖", "偏移量分钟": 0},
        ]
        analysis = analyze_topology(nodes, edges)
        self.assertEqual(len(analysis["环"]), 1)
        self.assertEqual(analysis["环"][0]["节点"], ["A", "B", "C"])
        self.assertEqual(analysis["孤立节点"], ["Z"])
        # 环上的节点拿不到层级与优先级，运行侧不能假装它们可调度。
        self.assertIsNone(analysis["调度优先级"]["A"])
        self.assertIsNone(analysis["调度优先级"]["Z"])

    def test_self_loop_detected(self) -> None:
        nodes = [{"节点编号": "A", "目标数据表": "t", "目标分区": "dt"}]
        analysis = analyze_topology(nodes, [{"上游节点": "A", "下游节点": "A"}])
        self.assertEqual(len(analysis["环"]), 1)

    def test_parallel_degree(self) -> None:
        nodes = [{"节点编号": key, "目标数据表": f"t_{key}", "目标分区": "dt"} for key in "ABCD"]
        edges = [
            {"上游节点": "A", "下游节点": "C"},
            {"上游节点": "B", "下游节点": "C"},
            {"上游节点": "C", "下游节点": "D"},
        ]
        analysis = analyze_topology(nodes, edges)
        self.assertEqual(analysis["起始节点数"], 2)
        self.assertEqual(analysis["最大并行度"], 2)
        self.assertEqual(analysis["调度优先级"]["D"], 1)

    def test_dangling_edge_reported(self) -> None:
        nodes = [{"节点编号": "A", "目标数据表": "t", "目标分区": "dt"}]
        analysis = analyze_topology(nodes, [{"上游节点": "A", "下游节点": "MISSING"}])
        self.assertEqual(analysis["悬空依赖"][0]["缺失节点"], ["MISSING"])


class DagPublishTest(unittest.TestCase):
    def setUp(self) -> None:
        self.svc = DagService()
        entry, missing = self.svc.create_entry({"图编号": "T-DAG", "图名称": "测试编排", "拓扑结构": "raw"})
        self.assertFalse(missing)
        self.dag_id = int(entry["id"])

    def _save(self, no: str, nodes: list[dict], edges: list[dict] | None = None) -> None:
        row, message = self.svc.save_canvas(
            self.dag_id,
            {"保存编号": no, "节点列表": nodes, "依赖边": edges or []},
        )
        self.assertIsNotNone(row, message)

    def test_blocked_precheck_explains_cycle_and_retry(self) -> None:
        self._save(
            "S1",
            [node(self.dag_id, "S1", "A"), node(self.dag_id, "S1", "B"), node(self.dag_id, "S1", "C")],
            [
                edge(self.dag_id, "S1", "A", "B"),
                edge(self.dag_id, "S1", "B", "A"),
            ],
        )
        record = self.svc.precheck(self.dag_id)
        self.assertEqual(record["状态"], "阻断")
        self.assertTrue(any("检测到环" in item for item in record["阻断原因"]))
        self.assertTrue(any("孤立节点" in item for item in record["阻断原因"]))
        # 阻断时不允许发布。
        snapshot, message = self.svc.publish(self.dag_id, {})
        self.assertIsNone(snapshot)
        self.assertIn("不能发布", message)
        # 修正画布后重试通过，重试次数被记录。旧的反向边必须显式撤销，
        # 否则按版本合并时它仍然存在，环也算真实存在而不是被“最后一次画布”抹掉。
        self._save(
            "S2",
            [node(self.dag_id, "S2", "A", version=2), node(self.dag_id, "S2", "B", version=2),
             node(self.dag_id, "S2", "C", version=2)],
            [edge(self.dag_id, "S2", "A", "B", version=2),
             edge(self.dag_id, "S2", "B", "C", version=2),
             edge(self.dag_id, "S2", "B", "A", version=2, revoked=True)],
        )
        retry = self.svc.precheck(self.dag_id)
        self.assertEqual(retry["状态"], "通过")
        self.assertGreaterEqual(retry["重试次数"], 1)

    def test_publish_requires_fresh_precheck(self) -> None:
        self._save("S1", [node(self.dag_id, "S1", "A"), node(self.dag_id, "S1", "B")],
                   [edge(self.dag_id, "S1", "A", "B")])
        self.assertEqual(self.svc.precheck(self.dag_id)["状态"], "通过")
        # 预检后画布再保存：旧结论作废，发布必须重新预检。
        self._save("S2", [node(self.dag_id, "S2", "A", version=2),
                          node(self.dag_id, "S2", "B", version=2),
                          node(self.dag_id, "S2", "C", version=1)],
                   [edge(self.dag_id, "S2", "A", "B", version=2)])
        snapshot, message = self.svc.publish(self.dag_id, {})
        self.assertIsNone(snapshot)
        self.assertIn("重新执行拓扑预检", message)
        self.assertEqual(self.svc.partition_dependencies(self.dag_id)["状态"], "待重检")

    def test_failed_precheck_keeps_old_snapshot_executable(self) -> None:
        self._save("S1", [node(self.dag_id, "S1", "A"), node(self.dag_id, "S1", "B")],
                   [edge(self.dag_id, "S1", "A", "B")])
        self.assertEqual(self.svc.precheck(self.dag_id)["状态"], "通过")
        snapshot, _ = self.svc.publish(self.dag_id, {})
        self.assertIsNotNone(snapshot)
        old_no = snapshot["快照编号"]

        self._save("S2", [node(self.dag_id, "S2", "A", version=2),
                          node(self.dag_id, "S2", "B", version=2),
                          node(self.dag_id, "S2", "ORPHAN", version=1)],
                   [edge(self.dag_id, "S2", "A", "B", version=2)])
        blocked = self.svc.precheck(self.dag_id)
        self.assertEqual(blocked["状态"], "阻断")
        view = self.svc.partition_dependencies(self.dag_id)
        self.assertEqual(view["状态"], "阻断")
        self.assertEqual(view["可使用快照"], old_no)
        self.assertEqual(view["旧快照状态"], "仍可执行")
        # 旧快照没有被覆盖，也没有被新拓扑替换。
        self.assertEqual(self.svc.active_snapshot(self.dag_id)["快照编号"], old_no)

    def test_snapshot_contains_partition_objects(self) -> None:
        self._save("S1", [node(self.dag_id, "S1", "A"), node(self.dag_id, "S1", "B")],
                   [edge(self.dag_id, "S1", "A", "B")])
        self.svc.precheck(self.dag_id)
        snapshot, _ = self.svc.publish(self.dag_id, {})
        self.assertIsNotNone(snapshot)
        view = self.svc.partition_dependencies(self.dag_id)
        self.assertEqual(view["状态"], "生效")
        self.assertEqual(len(view["分区依赖数据"]), 2)
        rows = {item["产出节点"]: item for item in view["分区依赖数据"]}
        self.assertEqual(rows["A"]["受影响节点"], ["A", "B"])
        self.assertEqual(len(view["失败传播清单"]), 2)
        self.assertFalse(rows["A"]["是否在环"])
        self.assertFalse(rows["A"]["是否孤立"])

    def test_new_topology_supersedes_old_snapshot(self) -> None:
        self._save("S1", [node(self.dag_id, "S1", "A"),
                          node(self.dag_id, "S1", "B", table="dwd.b_v1")],
                   [edge(self.dag_id, "S1", "A", "B")])
        self.svc.precheck(self.dag_id)
        first, _ = self.svc.publish(self.dag_id, {})
        self._save("S2", [node(self.dag_id, "S2", "A", version=2), node(self.dag_id, "S2", "B", version=2)])
        self.svc.precheck(self.dag_id)
        second, _ = self.svc.publish(self.dag_id, {})
        self.assertIsNotNone(second)
        active = self.svc.list_snapshots(self.dag_id)
        self.assertEqual(sum(1 for row in active if row["状态"] == "生效"), 1)
        self.assertEqual(active[0]["快照编号"], second["快照编号"])
        self.assertEqual(first["快照编号"], [r for r in active if r["状态"] == "已替换"][0]["快照编号"])

    def test_concurrent_publish_only_one_wins(self) -> None:
        self._save("S1", [node(self.dag_id, "S1", "A"), node(self.dag_id, "S1", "B")],
                   [edge(self.dag_id, "S1", "A", "B")])
        self.svc.precheck(self.dag_id)

        def attempt(_: int):
            return self.svc.publish(self.dag_id, {})

        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(attempt, range(8)))
        winners = [item for item in results if item[0] is not None]
        self.assertEqual(len(winners), 8)  # 幂等返回同一份快照
        snapshot_nos = {item[0]["快照编号"] for item in winners}
        self.assertEqual(len(snapshot_nos), 1)  # 但只有一份拓扑快照真正生效
        self.assertEqual(len(self.svc.list_snapshots(self.dag_id)), 1)
        self.assertEqual(
            sum(1 for row in self.svc.list_snapshots(self.dag_id) if row["状态"] == "生效"), 1
        )


class RuntimePropagationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.dags = DagService()
        self.runtime = RuntimeService()
        entry, _ = self.dags.create_entry({"图编号": "T-RUN", "图名称": "测试运行", "拓扑结构": "raw"})
        self.dag_id = int(entry["id"])
        self.dags.save_canvas(
            self.dag_id,
            {
                "保存编号": "R1",
                "节点列表": [node(self.dag_id, "R1", "A"), node(self.dag_id, "R1", "B")],
                "依赖边": [edge(self.dag_id, "R1", "A", "B")],
            },
        )
        self.dags.precheck(self.dag_id)
        self.dags.publish(self.dag_id, {})
        run, missing = self.runtime.create_entry(
            {"记录编号": "T-RUN-1", "作业名称": "测试作业", "开始时间": "2026-09-01",
             "编排图编号": self.dag_id}
        )
        self.assertFalse(missing)
        self.run_id = int(run["id"])

    def test_generate_and_read(self) -> None:
        record, message, view = self.runtime.generate_propagation(self.run_id)
        self.assertIsNotNone(record, message)
        self.assertEqual(view["状态"], "生效")
        self.assertEqual(len(view["失败传播清单"]), 2)
        self.assertEqual(len(view["分区依赖数据"]), 2)
        self.assertFalse(view["可重试"])

    def test_state_follows_precheck_failure(self) -> None:
        self.runtime.generate_propagation(self.run_id)
        self.assertEqual(self.runtime.propagation_view(self.run_id)["状态"], "生效")

        # 新增一个孤立节点 → 预检阻断，运行侧必须立刻跟着变化。
        self.dags.save_canvas(
            self.dag_id,
            {
                "保存编号": "R2",
                "节点列表": [node(self.dag_id, "R2", "A", version=2),
                             node(self.dag_id, "R2", "B", version=2),
                             node(self.dag_id, "R2", "ORPHAN", version=1)],
                "依赖边": [edge(self.dag_id, "R2", "A", "B", version=2)],
            },
        )
        blocked = self.dags.precheck(self.dag_id)
        self.assertEqual(blocked["状态"], "阻断")
        view = self.runtime.propagation_view(self.run_id)
        self.assertEqual(view["状态"], "阻断")
        self.assertTrue(any("孤立节点" in item for item in view["阻断原因"]))
        self.assertTrue(view["可重试"])
        # 旧快照仍可执行，但明确标注不是最新拓扑。
        self.assertEqual(view["旧快照状态"], "仍可执行")

        # 预检失败时重试生成会留下带原因的记录，而不是空清单。
        record, message, retry_view = self.runtime.generate_propagation(self.run_id)
        self.assertIsNone(record)
        self.assertIn("孤立节点", message)
        self.assertEqual(retry_view["状态"], "阻断")
        history = self.runtime.list_propagations(self.run_id)
        self.assertGreaterEqual(len(history), 2)
        self.assertTrue(history[0]["阻断原因"])

    def test_recovers_after_fix(self) -> None:
        self.runtime.generate_propagation(self.run_id)
        self.dags.save_canvas(
            self.dag_id,
            {
                "保存编号": "R3",
                "节点列表": [node(self.dag_id, "R3", "A", version=3),
                             node(self.dag_id, "R3", "B", version=3),
                             node(self.dag_id, "R3", "C", version=3)],
                "依赖边": [edge(self.dag_id, "R3", "A", "B", version=3),
                           edge(self.dag_id, "R3", "B", "C", version=3)],
            },
        )
        self.assertEqual(self.dags.precheck(self.dag_id)["状态"], "通过")
        # 预检恢复但还没发布：状态「待发布」，不能假装清单已生效。
        pending = self.runtime.propagation_view(self.run_id)
        self.assertEqual(pending["状态"], "待发布")
        self.dags.publish(self.dag_id, {})
        record, message, view = self.runtime.generate_propagation(self.run_id)
        self.assertIsNotNone(record, message)
        self.assertEqual(view["状态"], "生效")
        self.assertEqual(len(view["失败传播清单"]), 3)

    def test_unlinked_run_is_explained(self) -> None:
        run, _ = self.runtime.create_entry(
            {"记录编号": "T-RUN-2", "作业名称": "无编排图", "开始时间": "2026-09-02"}
        )
        view = self.runtime.propagation_view(int(run["id"]))
        self.assertEqual(view["状态"], "未关联编排图")
        self.assertTrue(view["阻断原因"])


if __name__ == "__main__":
    unittest.main()
