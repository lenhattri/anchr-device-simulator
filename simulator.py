"""
ANCHR Pump Simulator — asyncio + shared MQTT client pool + dynamic scaling.

Designed to run locally against an EMQX broker (e.g. on EKS via NLB).
Supports 1 000–5 000 simulated pumps with minimal CPU usage.
"""

import argparse
import asyncio
import json
import logging
import os
import random
import signal
import uuid
from datetime import datetime, timezone
from enum import IntEnum
from pathlib import Path

import paho.mqtt.client as mqtt
from aiohttp import web

try:
    import yaml
except ImportError:
    yaml = None

# Multi-station constants
NUM_STATIONS = 40
PUMPS_PER_STATION = 5

# ---------------------------------------------------------------------------
# Configuration: config.yaml > environment variables > defaults
# ---------------------------------------------------------------------------
def _load_config():
    """Load config from YAML file, then overlay with env vars."""
    defaults = {
        "mqtt_host": "localhost",
        "mqtt_port": 1883,
        "mqtt_tls": False,
        "tenant_id": "default",
        "station_id": "st-0007", # Kept for backward compat, but overridden by multi-station logic
        "speed": 10,
        "initial_pumps": 1000,
        "min_pumps": 1000,
        "max_pumps": 5000,
        "step_size": 1000,
        "scale_interval": 1800,
        "pool_size": 10,
    }

    # Parse --config flag
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-c", "--config", default="config.yaml")
    args, _ = parser.parse_known_args()

    # Load YAML if available
    cfg_path = Path(args.config)
    if yaml and cfg_path.exists():
        with open(cfg_path) as f:
            raw = yaml.safe_load(f) or {}
        # Flatten nested YAML sections
        mqtt_cfg = raw.get("mqtt", {})
        sim_cfg = raw.get("simulation", {})
        pool_cfg = raw.get("pool", {})
        defaults.update({
            "mqtt_host": mqtt_cfg.get("host", defaults["mqtt_host"]),
            "mqtt_port": mqtt_cfg.get("port", defaults["mqtt_port"]),
            "mqtt_tls": mqtt_cfg.get("tls", defaults["mqtt_tls"]),
            "tenant_id": sim_cfg.get("tenant_id", defaults["tenant_id"]),
            "station_id": sim_cfg.get("station_id", defaults["station_id"]),
            "speed": sim_cfg.get("speed", defaults["speed"]),
            "initial_pumps": sim_cfg.get("initial_pumps", defaults["initial_pumps"]),
            "min_pumps": sim_cfg.get("min_pumps", defaults["min_pumps"]),
            "max_pumps": sim_cfg.get("max_pumps", defaults["max_pumps"]),
            "step_size": sim_cfg.get("step_size", defaults["step_size"]),
            "scale_interval": sim_cfg.get("scale_interval", defaults["scale_interval"]),
            "pool_size": pool_cfg.get("size", defaults["pool_size"]),
        })

    # Env vars override everything
    def _env(key, default, cast=str):
        val = os.environ.get(key.upper())
        if val is None:
            return default
        if cast is bool:
            return val == "1" or val.lower() == "true"
        return cast(val)

    return {
        "mqtt_host": _env("MQTT_HOST", defaults["mqtt_host"]),
        "mqtt_port": _env("MQTT_PORT", defaults["mqtt_port"], int),
        "mqtt_tls": _env("MQTT_TLS", defaults["mqtt_tls"], bool),
        "tenant_id": _env("TENANT_ID", defaults["tenant_id"]),
        "station_id": _env("STATION_ID", defaults["station_id"]),
        "speed": _env("SPEED", defaults["speed"], int),
        "initial_pumps": _env("INITIAL_PUMPS", defaults["initial_pumps"], int),
        "min_pumps": _env("MIN_PUMPS", defaults["min_pumps"], int),
        "max_pumps": _env("MAX_PUMPS", defaults["max_pumps"], int),
        "step_size": _env("STEP_SIZE", defaults["step_size"], int),
        "scale_interval": _env("SCALE_INTERVAL", defaults["scale_interval"], int),
        "pool_size": _env("POOL_SIZE", defaults["pool_size"], int),
    }

CFG = _load_config()

MQTT_HOST = CFG["mqtt_host"]
MQTT_PORT = CFG["mqtt_port"]
MQTT_TLS = CFG["mqtt_tls"]
TENANT_ID = CFG["tenant_id"]
STATION_ID = CFG["station_id"] # Legacy single-station ID
SPEED = CFG["speed"]
INITIAL_PUMPS = CFG["initial_pumps"]
MIN_PUMPS = CFG["min_pumps"]
MAX_PUMPS = CFG["max_pumps"]
STEP_SIZE = CFG["step_size"]
SCALE_INTERVAL = CFG["scale_interval"]
POOL_SIZE = CFG["pool_size"]

# Sequence log for end-to-end reconciliation (optional)
SEQ_LOG_FILE = os.environ.get("SEQ_LOG_FILE", "")
_seq_log_handle = None
if SEQ_LOG_FILE:
    _seq_log_handle = open(SEQ_LOG_FILE, "a", buffering=8192)

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pump state machine
# ---------------------------------------------------------------------------
class PumpState(IntEnum):
    HOOK = 0
    LIFT = 1
    PUMP = 2
    ERROR = 9


# ---------------------------------------------------------------------------
# MQTT Client Pool
# ---------------------------------------------------------------------------
class MQTTClientPool:
    """A fixed-size pool of paho MQTT clients sharing the load."""

    def __init__(self, size, host, port, use_tls=False):
        self.size = size
        self.clients = []
        self.host = host
        self.port = port
        self.use_tls = use_tls

    def start(self):
        for i in range(self.size):
            client = mqtt.Client(client_id=f"sim-pool-{i}-{uuid.uuid4().hex[:6]}")
            client.on_connect = self._on_connect
            client.on_disconnect = self._on_disconnect
            client.reconnect_delay_set(min_delay=1, max_delay=30)
            if self.use_tls:
                client.tls_set()
                client.tls_insecure_set(True) # Added for dev envs
            client.connect_async(self.host, self.port, keepalive=60)
            client.loop_start()
            self.clients.append(client)
        logger.info("MQTT pool started: %d clients -> %s:%d", self.size, self.host, self.port)

    def stop(self):
        for c in self.clients:
            c.loop_stop()
            c.disconnect()

    def get(self, index):
        return self.clients[index % self.size]

    def subscribe(self, topic, qos, callback):
        """Subscribe on ALL pool clients so commands are received regardless of routing."""
        for c in self.clients:
            c.message_callback_add(topic, callback)
            c.subscribe(topic, qos=qos)

    # -- callbacks ----------------------------------------------------------
    @staticmethod
    def _on_connect(client, userdata, flags, rc):
        if rc == 0:
            logger.debug("Pool client %s connected.", client._client_id.decode())
        else:
            logger.warning("Pool client %s connect rc=%d", client._client_id.decode(), rc)

    @staticmethod
    def _on_disconnect(client, userdata, rc):
        if rc != 0:
            logger.warning("Pool client %s unexpected disconnect rc=%d", client._client_id.decode(), rc)


# ---------------------------------------------------------------------------
# Single Pump Coroutine
# ---------------------------------------------------------------------------
class PumpSim:
    """Lightweight pump simulation driven by an asyncio coroutine."""

    def __init__(self, pump_index, pool):
        # Calculate Station and Pump ID for Multi-station support
        # pump_index 0 -> st-0001:p-0001
        
        # Determine station index (0-based)
        station_idx = (pump_index // PUMPS_PER_STATION)
        # Cycle through 10 stations if index exceeds 1000
        station_num = (station_idx % NUM_STATIONS) + 1
        
        # Determine pump number (1-based within station)
        pump_num = (pump_index % PUMPS_PER_STATION) + 1
        
        # Override standard IDs
        self.station_id = f"st-{station_num:04d}"
        self.pump_id = f"p-{pump_num:04d}"
        self.device_id = f"{self.station_id}:{self.pump_id}"

        self.pool = pool
        self.client = pool.get(pump_index)

        # state
        self.state = PumpState.HOOK
        self.shift_no = 1
        self.unit_price = 20000 + (station_num * 50) # Slight variance per station
        self.currency = "VND"
        self.fuel_grade = "RON95"

        # transaction
        self.session_id = None
        self.start_time = None
        self.current_volume = 0.0
        self.current_amount = 0.0
        self.pump_rate = 0.5 + (random.random() * 0.5) # 0.5 - 1.0 L/s
        self.tx_seq = 0

        # display
        self.display_volume = 0.0
        self.display_amount = 0.0

        # totalizer
        self.totalizer = 10000.0 + (pump_index * 10)
        self.start_totalizer = 0.0

        self.msg_seq = 0

        # Subscribe to commands on pool client
        cmd_topic = f"anchr/v1/{TENANT_ID}/{self.station_id}/{self.pump_id}/cmd"
        pool.subscribe(cmd_topic, qos=1, callback=self._on_message)

    # -- MQTT helpers -------------------------------------------------------
    def _publish(self, subtopic, data, qos=0):
        topic = f"anchr/v1/{TENANT_ID}/{self.station_id}/{self.pump_id}/{subtopic}"
        self.msg_seq += 1
        envelope = {
            "schema": f"anchr.{subtopic}.v1",
            "schema_version": 1,
            "message_id": str(uuid.uuid4()),
            "type": subtopic,
            "tenant_id": TENANT_ID,
            "station_id": self.station_id,
            "pump_id": self.pump_id,
            "device_id": self.device_id,
            "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "seq": self.msg_seq,
            "data": data,
        }
        self.client.publish(topic, json.dumps(envelope), qos=qos)
        # Optional: write seq log for end-to-end reconciliation
        if _seq_log_handle is not None:
            _seq_log_handle.write(json.dumps({
                "device_id": self.device_id,
                "seq": self.msg_seq,
                "type": subtopic,
                "message_id": envelope["message_id"],
                "ts": envelope["event_time"]
            }) + "\n")

    def _publish_telemetry(self):
        vol = self.current_volume if self.state == PumpState.PUMP else self.display_volume
        amt = self.current_amount if self.state == PumpState.PUMP else self.display_amount
        self._publish(
            "telemetry",
            {
                "state_code": int(self.state),
                "display_amount": int(round(amt)),
                "display_volume_liters": round(vol, 3),
                "pump_rate_lpm": round(self.pump_rate * 60, 1), # Added field for UI
                "unit_price": self.unit_price,
                "shift_no": self.shift_no,
                "fuel_grade": self.fuel_grade,
                "currency": self.currency,
                "session_id": self.session_id,
                "meter_totalizer_liters": round(
                    self.totalizer + (self.current_volume if self.state == PumpState.PUMP else 0), 3
                ),
                "fw_version": "sim-1.0",
                "health": {"ok": True},
            },
            qos=0,
        )

    def _publish_transaction(self):
        end_time = datetime.now(timezone.utc)
        self.tx_seq += 1
        tx_id = f"{self.device_id}:{self.shift_no}:{self.tx_seq}"
        try:
            duration = int((end_time.timestamp() - self.start_time.timestamp()) * 1000)
        except Exception:
            duration = 0
        self._publish(
            "tx",
            {
                "tx_id": tx_id,
                "tx_seq": self.tx_seq,
                "shift_no": self.shift_no,
                "start_time": self.start_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "end_time": end_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "duration_ms": duration,
                "fuel_grade": self.fuel_grade,
                "unit_price": self.unit_price,
                "total_volume_liters": round(self.current_volume, 3),
                "total_amount": int(round(self.current_amount)),
                "currency": self.currency,
                "stop_reason": "manual_stop",
                "meter_start_totalizer_liters": round(self.start_totalizer, 3),
                "meter_end_totalizer_liters": round(self.totalizer, 3),
                "status": "COMPLETED",
            },
            qos=1,
        )
        logger.info("[%s] TX %s — %.3f L", self.device_id, tx_id, self.current_volume)

    # -- command handling ---------------------------------------------------
    def _on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode())
            self._handle_command(payload)
        except Exception as e:
            logger.error("[%s] cmd error: %s", self.device_id, e)

    def _handle_command(self, envelope):
        data = envelope.get("data", {})
        cmd = data.get("command", {})
        cmd_type = cmd.get("type")
        cmd_id = cmd.get("cmd_id")

        ack_status, ack_code, ack_msg, ack_payload = "REJECTED", 1, "Unknown command", {}

        if cmd_type == "set_price":
            if self.state == PumpState.HOOK:
                new_price = cmd.get("payload", {}).get("unit_price")
                if new_price:
                    self.unit_price = new_price
                    ack_status, ack_code, ack_msg = "APPLIED", 0, "Price updated"
                    ack_payload = {"unit_price": self.unit_price}
                else:
                    ack_msg = "Invalid price payload"
            else:
                ack_msg = "Cannot change price while pumping"
        elif cmd_type == "close_shift":
            if self.state == PumpState.HOOK:
                old_shift = self.shift_no
                self.shift_no += 1
                ack_status, ack_code, ack_msg = "APPLIED", 0, "Shift closed"
                ack_payload = {"old_shift_no": old_shift, "new_shift_no": self.shift_no}
            else:
                ack_msg = "Cannot close shift while pumping"

        ack_topic = f"anchr/v1/{TENANT_ID}/{self.station_id}/{self.pump_id}/ack"
        self.msg_seq += 1
        ack_envelope = {
            "schema": "anchr.ack.v1",
            "schema_version": 1,
            "message_id": str(uuid.uuid4()),
            "type": "ack",
            "tenant_id": TENANT_ID,
            "station_id": self.station_id, # UPDATED to use instance station_id
            "pump_id": self.pump_id,
            "device_id": self.device_id,
            "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "seq": self.msg_seq,
            "correlation_id": cmd_id,
            "data": {
                "ack": {
                    "cmd_id": cmd_id,
                    "type": cmd_type,
                    "status": ack_status,
                    "code": ack_code,
                    "message": ack_msg,
                    "applied_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    "payload": ack_payload,
                }
            },
        }
        self.client.publish(ack_topic, json.dumps(ack_envelope), qos=1)

    # -- main coroutine -----------------------------------------------------
    async def run(self):
        """State-machine loop for one pump."""
        # Staggered start to avoid thundering herd
        await asyncio.sleep(random.uniform(0, 10))
        logger.debug("[%s] started", self.device_id)

        while True:
            if self.state == PumpState.HOOK:
                idle_ticks = random.randint(5, 10)
                for _ in range(idle_ticks):
                    self._publish_telemetry()
                    await asyncio.sleep(1.0 / SPEED)

                # -> LIFT
                self.state = PumpState.LIFT
                self.session_id = str(uuid.uuid4())
                self.start_totalizer = self.totalizer
                self.current_volume = 0.0
                self.current_amount = 0.0

            elif self.state == PumpState.LIFT:
                self._publish_telemetry()
                await asyncio.sleep(2.0 / SPEED)

                # -> PUMP
                self.state = PumpState.PUMP
                self.start_time = datetime.now(timezone.utc)
                self.display_volume = 0.0
                self.display_amount = 0.0

            elif self.state == PumpState.PUMP:
                pump_ticks = random.randint(5, 15)
                for _ in range(pump_ticks):
                    self.current_volume += self.pump_rate
                    self.current_amount = self.current_volume * self.unit_price
                    self._publish_telemetry()
                    await asyncio.sleep(1.0 / SPEED)

                self.totalizer += self.current_volume
                self.display_volume = self.current_volume
                self.display_amount = self.current_amount
                self._publish_transaction()

                # -> HOOK
                self.state = PumpState.HOOK
                self.session_id = None


# ---------------------------------------------------------------------------
# Scale Manager
# ---------------------------------------------------------------------------
class ScaleManager:
    """Periodically adjusts the number of running pump coroutines."""

    def __init__(self, pool):
        self.pool = pool
        self.tasks = {}  # pump_index -> Task
        self.target = INITIAL_PUMPS

    async def run(self):
        # Initial ramp-up
        self._scale_to(self.target)
        logger.info("ScaleManager: initial pumps=%d, interval=%ds, step=%d", self.target, SCALE_INTERVAL, STEP_SIZE)

        while True:
            await asyncio.sleep(SCALE_INTERVAL)
            # Next target
            next_target = self.target + STEP_SIZE
            if next_target > MAX_PUMPS:
                next_target = MIN_PUMPS
            self.target = next_target
            self._scale_to(self.target)

    def _scale_to(self, count):
        current = len(self.tasks)
        if count > current:
            # scale up
            for idx in range(current, count): # FIXED: range(current, count) -> 0 to count-1 if current=0
                pump = PumpSim(idx, self.pool)
                task = asyncio.get_event_loop().create_task(pump.run())
                self.tasks[idx] = task
            logger.info("Scaled UP: %d -> %d pumps", current, count)
        elif count < current:
            # scale down — cancel highest-indexed pumps
            to_remove = sorted(self.tasks.keys(), reverse=True)[: current - count]
            for idx in to_remove:
                self.tasks[idx].cancel()
                del self.tasks[idx]
            logger.info("Scaled DOWN: %d -> %d pumps", current, count)
        else:
            logger.info("Scale unchanged: %d pumps", count)


# ---------------------------------------------------------------------------
# HTTP API for external scale control
# ---------------------------------------------------------------------------
class HTTPApi:
    """HTTP API for external control of simulator (from EKS cluster)."""

    def __init__(self, manager: ScaleManager):
        self.manager = manager
        self.app = web.Application()
        self.app.router.add_get("/api/status", self.handle_status)
        self.app.router.add_post("/api/scale", self.handle_scale)
        self.app.router.add_get("/healthz", self.handle_health)

    async def handle_health(self, request):
        return web.Response(text="ok")

    async def handle_status(self, request):
        return web.json_response({
            "current_pumps": len(self.manager.tasks),
            "target": self.manager.target,
            "min_pumps": MIN_PUMPS,
            "max_pumps": MAX_PUMPS,
            "mqtt_host": MQTT_HOST,
            "mqtt_port": MQTT_PORT,
        })

    async def handle_scale(self, request):
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "invalid json"}, status=400)

        target = data.get("target_pumps")
        if target is None:
            return web.json_response({"error": "target_pumps required"}, status=400)

        if not isinstance(target, int):
            return web.json_response({"error": "target_pumps must be integer"}, status=400)

        if not MIN_PUMPS <= target <= MAX_PUMPS:
            return web.json_response({
                "error": f"target_pumps must be between {MIN_PUMPS} and {MAX_PUMPS}"
            }, status=400)

        old_count = len(self.manager.tasks)
        self.manager.target = target
        self.manager._scale_to(target)

        return web.json_response({
            "status": "ok",
            "previous_pumps": old_count,
            "target_pumps": target,
            "current_pumps": len(self.manager.tasks),
        })

    async def start(self, host="0.0.0.0", port=8080):
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, host, port)
        await site.start()
        logger.info("HTTP API started on %s:%d", host, port)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
async def main():
    logger.info(
        "ANCHR Pump Simulator starting — host=%s:%d  initial=%d  pool=%d  speed=%dx",
        MQTT_HOST, MQTT_PORT, INITIAL_PUMPS, POOL_SIZE, SPEED,
    )

    pool = MQTTClientPool(POOL_SIZE, MQTT_HOST, MQTT_PORT, use_tls=MQTT_TLS)
    pool.start()

    # Give pool clients a moment to connect
    await asyncio.sleep(2)

    manager = ScaleManager(pool)

    # Start HTTP API for external control (from EKS cluster)
    api = HTTPApi(manager)
    await api.start(port=8080)

    loop = asyncio.get_event_loop()
    stop_event = asyncio.Event()

    def _shutdown():
        logger.info("Shutdown signal received.")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown)

    manager_task = asyncio.create_task(manager.run())

    # Wait until shutdown
    await stop_event.wait()

    # Cleanup
    manager_task.cancel()
    for t in manager.tasks.values():
        t.cancel()
    # Let cancellations propagate
    await asyncio.sleep(0.5)
    pool.stop()
    logger.info("Simulator shut down.")


if __name__ == "__main__":
    asyncio.run(main())
