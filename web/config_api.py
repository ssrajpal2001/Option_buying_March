import json
import os
from pathlib import Path
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Any

router = APIRouter()
STRATEGY_FILE = Path("config/strategy_logic.json")


def _load_strategy():
    if not STRATEGY_FILE.exists():
        raise HTTPException(status_code=404, detail="strategy_logic.json not found")
    with open(STRATEGY_FILE, "r") as f:
        return json.load(f)


def _save_strategy(data: dict):
    if "NIFTY" not in data:
        raise HTTPException(status_code=400, detail="Invalid config: missing NIFTY key")
    tmp = STRATEGY_FILE.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(data, f, indent=4)
    os.replace(tmp, STRATEGY_FILE)


@router.get("/strategy")
async def get_strategy():
    return JSONResponse(_load_strategy())


class StrategyUpdate(BaseModel):
    data: Any


@router.post("/strategy")
async def update_strategy(payload: StrategyUpdate):
    _save_strategy(payload.data)
    return JSONResponse({"success": True, "message": "Strategy saved. Changes take effect on next tick."})


@router.patch("/strategy/toggle")
async def toggle_setting(path: str, value: bool):
    data = _load_strategy()
    keys = path.split(".")
    node = data
    for k in keys[:-1]:
        if k not in node:
            raise HTTPException(status_code=400, detail=f"Key '{k}' not found")
        node = node[k]
    node[keys[-1]] = value
    _save_strategy(data)
    return JSONResponse({"success": True, "path": path, "value": value})


@router.patch("/strategy/value")
async def set_value(path: str, value: str):
    data = _load_strategy()
    keys = path.split(".")
    node = data
    for k in keys[:-1]:
        if k not in node:
            raise HTTPException(status_code=400, detail=f"Key '{k}' not found")
        node = node[k]
    try:
        parsed = json.loads(value)
    except Exception:
        parsed = value
    node[keys[-1]] = parsed
    _save_strategy(data)
    return JSONResponse({"success": True, "path": path, "value": parsed})
