import os
import signal
import subprocess
import time
from pathlib import Path
from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter()
PROJECT_ROOT = Path(__file__).parent.parent
BOT_PID_FILE = PROJECT_ROOT / "config" / "bot.pid"


def _find_bot_pid():
    if BOT_PID_FILE.exists():
        try:
            pid = int(BOT_PID_FILE.read_text().strip())
            os.kill(pid, 0)
            return pid
        except (ProcessLookupError, ValueError):
            BOT_PID_FILE.unlink(missing_ok=True)
    try:
        result = subprocess.run(
            ["pgrep", "-f", "python main.py"],
            capture_output=True, text=True
        )
        pids = [int(p) for p in result.stdout.strip().split() if p]
        return pids[0] if pids else None
    except Exception:
        return None


@router.get("/bot/status")
async def bot_status():
    pid = _find_bot_pid()
    return JSONResponse({"running": pid is not None, "pid": pid})


@router.post("/bot/restart")
async def restart_bot():
    pid = _find_bot_pid()
    if pid:
        try:
            os.kill(pid, signal.SIGTERM)
            time.sleep(2)
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        except ProcessLookupError:
            pass

    time.sleep(1)
    proc = subprocess.Popen(
        ["python", "main.py"],
        cwd=str(PROJECT_ROOT),
        stdout=open(PROJECT_ROOT / "app.log", "a"),
        stderr=subprocess.STDOUT,
        start_new_session=True
    )
    BOT_PID_FILE.write_text(str(proc.pid))
    return JSONResponse({"success": True, "pid": proc.pid, "message": "Bot restarted successfully."})


@router.post("/bot/stop")
async def stop_bot():
    pid = _find_bot_pid()
    if not pid:
        return JSONResponse({"success": False, "message": "Bot is not running."})
    try:
        os.kill(pid, signal.SIGTERM)
        time.sleep(2)
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        BOT_PID_FILE.unlink(missing_ok=True)
        return JSONResponse({"success": True, "message": "Bot stopped."})
    except Exception as e:
        return JSONResponse({"success": False, "message": str(e)})
