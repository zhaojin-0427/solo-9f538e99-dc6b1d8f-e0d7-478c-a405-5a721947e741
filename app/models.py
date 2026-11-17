"""Pydantic 响应/请求模型与统一错误类型。"""
from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from fastapi import HTTPException
from pydantic import BaseModel, Field, field_validator


class ConflictError(HTTPException):
    """业务规则冲突（409）：请求本身合法，但违反数仓不变量。"""

    def __init__(self, detail: str) -> None:
        super().__init__(status_code=409, detail=detail)


class GoneError(HTTPException):
    """资源已退役（410）：例如快照已被回收流程退役，明确拒绝再读。"""

    def __init__(self, detail: str) -> None:
        super().__init__(status_code=410, detail=detail)


class NotFound(HTTPException):
    """资源不存在（404）。"""

    def __init__(self, detail: str) -> None:
        super().__init__(status_code=404, detail=detail)


class IndicatorOut(BaseModel):
    indicator_id: int = Field(..., description="指标稳定身份 ID；改名不变，同名复用会得到新 ID")
    name: str = Field(..., description="当前指标名")
    active: bool


class SchemaOut(BaseModel):
    current_version: int = Field(..., description="当前字段版号")
    fixed_columns: List[str] = Field(..., description="固定字段")
    indicators: List[IndicatorOut] = Field(..., description="当前存续指标（按指标 ID 排序）")


class VersionIndicatorOut(BaseModel):
    indicator_id: int
    name: str
    active: bool


class VersionOut(BaseModel):
    version_id: int
    created_at: str
    change_note: str
    indicators: List[VersionIndicatorOut]


class RenameRequest(BaseModel):
    old_name: str = Field(..., min_length=1, description="当前指标名（例：浓度）")
    new_name: str = Field(..., min_length=1, description="新指标名（例：硝酸盐）")


class CreateIndicatorRequest(BaseModel):
    name: str = Field(..., min_length=1, description="新指标名；若与历史指标同名则视为全新指标，不继承旧值")


class IndicatorChangeResponse(BaseModel):
    indicator_id: int
    name: str
    active: bool
    new_version: int = Field(..., description="本次变更产生的字段版号")


class UploadResponse(BaseModel):
    shard_id: int
    schema_version: int = Field(..., description="本次上传指定并裁决所用的字段版")
    row_count: int
    stored_columns: List[str] = Field(..., description="分片实际落盘的指标身份列")
    file_path: str


class SnapshotMeta(BaseModel):
    snapshot_id: int
    created_at: str
    status: str = Field(..., description="快照状态：active 可分页读取；retired 已被回收退役，拒绝读取")
    retired_at: Optional[str] = Field(None, description="退役时刻（UTC）；存续快照为 null")
    indicator_names: List[str]
    shard_ids: List[int]


class RowPage(BaseModel):
    snapshot_id: int
    snapshot_created: bool = Field(..., description="本次调用是否新建了快照（首次查询为 true）")
    page: int
    page_size: int
    total_rows: int
    total_pages: int
    fixed_columns: List[str]
    indicator_names: List[str] = Field(..., description="快照固化的当前指标名；旧分片没有的指标为 null")
    rows: List[dict]


# ---------------------------------------------------------------------------
# 保留期回收
# ---------------------------------------------------------------------------

class ReclaimRequest(BaseModel):
    cutoff: datetime = Field(
        ...,
        description=(
            "UTC 截止时刻（必须带时区且不晚于当前时间）。"
            "退役该时刻前（含）创建的快照；此后新快照排除该时刻前（含）上传的分片。"
        ),
    )

    @field_validator("cutoff")
    @classmethod
    def _require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            # 与业务校验一致抛 409（而非默认 422），明确告知截止时刻的时区要求
            raise ConflictError("截止时刻 cutoff 必须带时区信息（UTC 偏移或 Z 后缀），不接受无时区时刻")
        return value


class DeletedShardOut(BaseModel):
    shard_id: int
    file_path: str
    row_count: int
    uploaded_at: str


class RetainedShardOut(BaseModel):
    shard_id: int
    file_path: str
    row_count: int
    uploaded_at: str
    referenced_by_snapshots: List[int] = Field(
        ..., description="仍引用该分片的保留（active）快照号"
    )


class ReclaimResponse(BaseModel):
    cutoff: str = Field(..., description="本次请求规范化后的 UTC 截止时刻")
    applied: bool = Field(..., description="本次是否真正推进了保留水位线（重复 cutoff 为 false）")
    retired_snapshot_ids: List[int] = Field(..., description="本次退役的快照号")
    deleted_shards: List[DeletedShardOut] = Field(..., description="已删除文件与登记的分片")
    retained_shards: List[RetainedShardOut] = Field(
        ..., description="截止时刻前但仍被保留快照引用、暂不能删除的分片"
    )
    pending_delete_shard_ids: List[int] = Field(
        ..., description="已标记待删、文件删除中断留待重试的分片"
    )
    active_cutoff: Optional[str] = Field(..., description="处理后当前生效的保留水位线（UTC）")


class RetentionOut(BaseModel):
    active_cutoff: Optional[str] = Field(..., description="当前生效保留水位线；从未回收则为 null")
    active_snapshot_ids: List[int]
    retired_snapshot_ids: List[int]
    active_shard_ids: List[int]
    pending_delete_shard_ids: List[int]
