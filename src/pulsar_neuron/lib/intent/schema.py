from __future__ import annotations

from pydantic import BaseModel, Field


class EntrySpec(BaseModel):
    type: str = Field(default="LIMIT")
    price: float | None = None


class SLSpec(BaseModel):
    type: str = Field(default="SL-M")
    level: float


class TPSpec(BaseModel):
    type: str = Field(default="LIMIT")
    level: float


class EntryIntent(BaseModel):
    ts: str
    symbol: str
    side: str
    product: str = "MIS"
    qty: int
    entry: EntrySpec
    sl: SLSpec
    tp: TPSpec | None = None
    rr: float | None = None
    reason: str = ""
    source: str = "index_v0"


def validate_intent(intent: EntryIntent) -> None:
    assert intent.qty > 0, "Quantity must be > 0"
