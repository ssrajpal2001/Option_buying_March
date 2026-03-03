import json
import time
from pathlib import Path
from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter()
STATUS_FILE = Path("config/bot_status.json")


@router.get("/status")
async def get_status():
    if not STATUS_FILE.exists():
        return JSONResponse({"bot_running": False, "reason": "Status file not found"})
    try:
        stat = STATUS_FILE.stat()
        age = time.time() - stat.st_mtime
        with open(STATUS_FILE, "r") as f:
            data = json.load(f)
        if age > 30:
            data["bot_running"] = False
            data["stale"] = True
            data["stale_seconds"] = round(age)
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"bot_running": False, "error": str(e)})
