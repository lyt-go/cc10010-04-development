"""任务编排接口：画布保存、拓扑预检、发布与分区依赖快照读取。

路由顺序有讲究：`/export` 这类固定路径必须放在 `/{entry_id}` 之前，否则会被
动态路径抢先匹配，导出接口直接 422。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.dag import DagService

router = APIRouter(prefix="/api/dag", tags=["任务编排"])

service = DagService()

LIST_FIELDS = ["图编号", "图名称", "拓扑结构", "起始节点数", "最大并行度", "失败传播策略", "调度优先级", "生效时间范围"]
STATUSES = ["草稿", "编排评审中", "已发布", "已停用"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按图编号检索"),
    status: str | None = Query(default=None, description="草稿、编排评审中、已发布、已停用"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按图编号与状态过滤任务编排列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出任务编排清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "dag", "total": total, "items": items}


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条编排图，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="编排图已登记", entry=entry)


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条编排图明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"编排图 {entry_id} 不存在或已归档")
    return entry


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条编排图执行提交编排评审、发布编排、停用编排；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


# ---------------------------------------------------------------- 画布保存


@router.post("/{entry_id}/saves", response_model=ActionResult)
def save_canvas(entry_id: int, payload: EntryPayload) -> ActionResult:
    """保存一次画布快照：多次保存会按节点 / 依赖边版本合并，不会覆盖历史保存。"""
    save_row, message = service.save_canvas(entry_id, payload.values)
    if save_row is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=save_row)


@router.get("/{entry_id}/saves")
def list_saves(entry_id: int) -> dict[str, Any]:
    """列出编排图的历次画布保存，便于定位版本冲突来自哪一次保存。"""
    if service.get_entry(entry_id) is None:
        raise HTTPException(status_code=404, detail=f"编排图 {entry_id} 不存在或已归档")
    saves = service.list_saves(entry_id)
    return {"items": saves, "total": len(saves)}


@router.get("/{entry_id}/merge")
def merge_preview(entry_id: int) -> dict[str, Any]:
    """查看多次保存合并后的节点、依赖边与版本决议（不执行预检、不落状态）。"""
    if service.get_entry(entry_id) is None:
        raise HTTPException(status_code=404, detail=f"编排图 {entry_id} 不存在或已归档")
    return service.merge_preview(entry_id)


# ---------------------------------------------------------------- 拓扑预检


@router.post("/{entry_id}/prechecks", response_model=ActionResult)
def run_precheck(entry_id: int, payload: EntryPayload | None = None) -> ActionResult:
    """执行拓扑预检：检测环、孤立节点、悬空依赖与缺失分区，失败时给出阻断原因。"""
    record = service.precheck(entry_id, payload.values if payload else {})
    if not record.get("预检编号"):
        return ActionResult(ok=False, message="；".join(record.get("阻断原因") or ["预检未执行"]), entry=record)
    ok = record.get("状态") == "通过"
    message = record.get("说明") or ""
    return ActionResult(ok=ok, message=message, entry=record)


@router.get("/{entry_id}/prechecks")
def list_prechecks(entry_id: int) -> dict[str, Any]:
    """预检历史：阻断原因与重试次数都保留，失败不会把记录抹掉。"""
    if service.get_entry(entry_id) is None:
        raise HTTPException(status_code=404, detail=f"编排图 {entry_id} 不存在或已归档")
    items = service.list_prechecks(entry_id)
    return {"items": items, "total": len(items)}


@router.get("/{entry_id}/prechecks/{precheck_id}")
def get_precheck(entry_id: int, precheck_id: int) -> dict[str, Any]:
    """读取单次预检明细；不存在时说明是哪张编排图的哪次预检。"""
    record = service.get_precheck(entry_id, precheck_id)
    if record is None:
        raise HTTPException(
            status_code=404,
            detail=f"编排图 {entry_id} 的预检记录 {precheck_id} 不存在",
        )
    return record


# ---------------------------------------------------------------- 发布与快照


@router.post("/{entry_id}/publish", response_model=ActionResult)
def publish(entry_id: int, payload: EntryPayload | None = None) -> ActionResult:
    """发布编排图：只有通过预检且画布未再变更的拓扑才能生成生效快照。"""
    snapshot, message = service.publish(entry_id, payload.values if payload else {})
    if snapshot is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=snapshot)


@router.get("/{entry_id}/snapshots")
def list_snapshots(entry_id: int) -> dict[str, Any]:
    """快照历史：只可能有一份状态为「生效」，旧快照保留用于追溯。"""
    if service.get_entry(entry_id) is None:
        raise HTTPException(status_code=404, detail=f"编排图 {entry_id} 不存在或已归档")
    items = service.list_snapshots(entry_id)
    return {"items": items, "total": len(items)}


@router.get("/{entry_id}/partition-dependencies")
def partition_dependencies(entry_id: int) -> dict[str, Any]:
    """分区依赖数据：优先读生效快照，没有快照时返回最近预检结论与阻断原因。"""
    return service.partition_dependencies(entry_id)
