"""拓扑预检的可解释性测试：并发发布、异常退出与运行侧联动。

这些用例直接构造「预检写到一半进程退出」和「并发发布抢锁」的状态，验证读取侧
永远给出可解释结论，不出现空清单。
"""
from __future__ import annotations

import unittest
from threading import Barrier, Thread
from unittest import mock

from app.services import dag as dag_module
from app.services import runtime as runtime_module
from app.services.dag import DagService
from app.services.runtime import RuntimeService
from app.store import store
from tests.test_topology_precheck import edge, node


class ExplainabilityTest(unittest.TestCase):
    def setUp(self) -> None:
        self.dags = DagService()
        self.runtime = RuntimeService()
        entry, _ = self.dags.create_entry({"图编号": "X-DAG", "图名称": "解释性", "拓扑结构": "raw"})
        self.dag_id = int(entry["id"])
        self.dags.save_canvas(
            self.dag_id,
            {
                "保存编号": "X1",
                "节点列表": [node(self.dag_id, "X1", "A"), node(self.dag_id, "X1", "B")],
                "依赖边": [edge(self.dag_id, "X1", "A", "B")],
            },
        )

    def test_interrupted_precheck_is_explained(self) -> None:
        # 模拟预检记录已经落库、但计算中途进程退出。
        store.rows("dagprecheck").append(
            {
                "id": store.next_id("dagprecheck"),
                "dag_id": self.dag_id,
                "预检编号": "PRE-X-INTERRUPTED",
                "状态": "运行中",
                "拓扑版本": self.dags.canvas_version(self.dag_id),
                "保存指纹": "",
                "阻断原因": [],
                "分区依赖数据": [],
                "失败传播清单": [],
            }
        )
        view = self.dags.partition_dependencies(self.dag_id)
        self.assertEqual(view["状态"], "预检中断")
        self.assertTrue(view["阻断原因"])
        self.assertTrue(view["可重试"])
        # 重新预检能恢复。
        self.assertEqual(self.dags.precheck(self.dag_id)["状态"], "通过")

    def test_precheck_exception_recorded(self) -> None:
        with mock.patch.object(dag_module, "assess", side_effect=RuntimeError("磁盘读取失败")):
            record = self.dags.precheck(self.dag_id)
        self.assertEqual(record["状态"], "异常")
        self.assertIn("磁盘读取失败", "；".join(record["阻断原因"]))
        snapshot, message = self.dags.publish(self.dag_id, {})
        self.assertIsNone(snapshot)
        self.assertIn("异常", message)

    def test_interrupted_generation_leaves_reason(self) -> None:
        self.dags.precheck(self.dag_id)
        self.dags.publish(self.dag_id, {})
        run, _ = self.runtime.create_entry(
            {"记录编号": "X-RUN", "作业名称": "作业", "开始时间": "2026-09-01",
             "编排图编号": self.dag_id}
        )
        with mock.patch.object(
            self.runtime,
            "dags",
            mock.Mock(partition_dependencies=mock.Mock(side_effect=RuntimeError("网络中断"))),
        ):
            record, message, view = self.runtime.generate_propagation(int(run["id"]))
        # 跨模块读取中断不能返回空结果，要留下异常记录并允许重试。
        self.assertIsNone(record)
        self.assertIn("网络中断", message)
        self.assertEqual(view["状态"], "异常")
        self.assertTrue(view["可重试"])
        history = self.runtime.list_propagations(int(run["id"]))
        self.assertEqual(history[0]["状态"], "异常")
        self.assertTrue(history[0]["阻断原因"])

    def test_two_prechecks_race_only_latest_publishes(self) -> None:
        self.dags.save_canvas(
            self.dag_id,
            {
                "保存编号": "X2",
                "节点列表": [node(self.dag_id, "X2", "A", version=2),
                             node(self.dag_id, "X2", "B", version=2),
                             node(self.dag_id, "X2", "C", version=1)],
                "依赖边": [edge(self.dag_id, "X2", "A", "B", version=2),
                           edge(self.dag_id, "X2", "B", "C", version=2)],
            },
        )
        barrier = Barrier(4)
        outcomes: list[tuple] = []

        def worker() -> None:
            barrier.wait()
            record = self.dags.precheck(self.dag_id)
            outcomes.append(self.dags.publish(self.dag_id, {"预检id": record["id"]}))

        threads = [Thread(target=worker) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        snapshots = self.dags.list_snapshots(self.dag_id)
        self.assertEqual(sum(1 for row in snapshots if row["状态"] == "生效"), 1)
        # 无论谁先抢到，生效快照都必须来自同一份通过预检的拓扑。
        active = self.dags.active_snapshot(self.dag_id)
        self.assertEqual(active["拓扑版本"], self.dags.canvas_version(self.dag_id))


if __name__ == "__main__":
    unittest.main()
