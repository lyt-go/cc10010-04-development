"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。

拓扑预检会同时写编排图状态、快照和失败传播清单，因此这里补了两个能力：

* ``transaction()``：整段写入要么全部可见、要么全部回滚，避免异常退出后留下
  编排图已发布但没有快照 / 没有传播清单的半截数据；
* ``RLock``：并发发布同一编排图时串行进入临界区，配合服务层的期望版本校验，
  保证同一编排图同一时刻只有一份生效拓扑快照。
"""
from __future__ import annotations

import copy
from threading import RLock
from typing import Any, Iterator

from app.seed import SEED_ROWS

# 拓扑预检的内部支撑表，不参与运营概览的业务模块统计。
INTERNAL_MODULES = {
    "dagsave",
    "dagnode",
    "dagedge",
    "dagprecheck",
    "dagsnapshot",
    "dagpropagation",
    "runpropagation",
    "topologyaudit",
}


class _StoreTransaction:
    """基于整表深拷贝的轻量事务：异常时回滚到进入事务前的内存状态。"""

    def __init__(self, owner: "Store") -> None:
        self._owner = owner
        self._snapshot: dict[str, list[dict[str, Any]]] | None = None

    def __enter__(self) -> "_StoreTransaction":
        self._owner.lock.acquire()
        self._snapshot = copy.deepcopy(self._owner._tables)
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        try:
            if exc_type is not None and self._snapshot is not None:
                self._owner._tables = self._snapshot
        finally:
            self._snapshot = None
            self._owner.lock.release()
        # 不吞异常，交给调用方决定如何给出可读的阻断原因。
        return False


class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [copy.deepcopy(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        self.lock = RLock()

    def transaction(self) -> _StoreTransaction:
        """进入一个串行、可回滚的写入事务（支持同线程嵌套获取锁）。"""
        return _StoreTransaction(self)

    def module_names(self) -> list[str]:
        """业务模块名（拓扑预检的内部表不对外当成业务模块）。"""
        return sorted(name for name in self._tables if name not in INTERNAL_MODULES)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def next_id(self, module: str) -> int:
        with self.lock:
            return max((int(row.get("id", 0)) for row in self.rows(module)), default=0) + 1

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}


store = Store()
