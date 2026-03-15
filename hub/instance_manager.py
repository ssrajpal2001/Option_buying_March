import os
import sys
import subprocess
import signal
from datetime import datetime, timezone
from utils.logger import logger


class InstanceManager:
    """
    Manages per-client bot subprocess instances.
    Each client gets an isolated Python process running main.py
    with client-specific config injected via environment variables.
    """

    def __init__(self):
        self._processes: dict[int, subprocess.Popen] = {}

    def start_instance(
        self,
        instance_id: int,
        client_id: int,
        username: str,
        broker: str,
        instrument: str,
        quantity: int,
        strategy_version: str,
        trading_mode: str,
        api_key: str,
        access_token: str,
    ) -> tuple[bool, str]:
        if instance_id in self._processes:
            proc = self._processes[instance_id]
            if proc.poll() is None:
                return False, "Instance is already running."
            del self._processes[instance_id]

        env = os.environ.copy()
        env.update({
            "CLIENT_ID": str(client_id),
            "CLIENT_USERNAME": username,
            "CLIENT_BROKER": broker,
            "CLIENT_INSTRUMENT": instrument,
            "CLIENT_QUANTITY": str(quantity),
            "CLIENT_STRATEGY_VERSION": strategy_version,
            "CLIENT_TRADING_MODE": trading_mode,
            "CLIENT_API_KEY": api_key,
            "CLIENT_ACCESS_TOKEN": access_token,
            "CLIENT_INSTANCE_ID": str(instance_id),
        })

        log_file = f"logs/client_{client_id}_{broker}.log"
        os.makedirs("logs", exist_ok=True)

        try:
            log_fh = open(log_file, "a")
            proc = subprocess.Popen(
                [sys.executable, "main.py", "--client_mode"],
                env=env,
                stdout=log_fh,
                stderr=log_fh,
            )
            self._processes[instance_id] = proc
            logger.info(f"[InstanceManager] Started instance {instance_id} (PID {proc.pid}) for client {username}")
            return True, f"Bot started (PID {proc.pid})"
        except Exception as e:
            logger.error(f"[InstanceManager] Failed to start instance {instance_id}: {e}")
            return False, f"Failed to start bot: {e}"

    def stop_instance(self, instance_id: int) -> tuple[bool, str]:
        proc = self._processes.get(instance_id)
        if not proc:
            return False, "No running instance found."
        if proc.poll() is not None:
            del self._processes[instance_id]
            return False, "Instance was not running."
        try:
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
            del self._processes[instance_id]
            logger.info(f"[InstanceManager] Stopped instance {instance_id}")
            return True, "Bot stopped."
        except Exception as e:
            logger.error(f"[InstanceManager] Error stopping instance {instance_id}: {e}")
            return False, str(e)

    def stop_all_for_client(self, client_id: int):
        from web.db import db_fetchall, db_execute
        instances = db_fetchall(
            "SELECT id FROM client_broker_instances WHERE client_id=?", (client_id,)
        )
        for row in instances:
            iid = row["id"]
            if iid in self._processes:
                self.stop_instance(iid)
                db_execute("UPDATE client_broker_instances SET status='idle', bot_pid=NULL WHERE id=?", (iid,))

    def get_instance_status(self, instance_id: int) -> dict:
        proc = self._processes.get(instance_id)
        if not proc:
            return {"running": False, "pid": None}
        alive = proc.poll() is None
        return {
            "running": alive,
            "pid": proc.pid if alive else None,
        }

    def list_running(self) -> list:
        result = []
        for iid, proc in list(self._processes.items()):
            if proc.poll() is None:
                result.append({"instance_id": iid, "pid": proc.pid})
            else:
                del self._processes[iid]
        return result


instance_manager = InstanceManager()
