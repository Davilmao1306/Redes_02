from __future__ import annotations

import os
import threading
import time
from contextlib import asynccontextmanager
from typing import Any

import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from common import DroneInfo, DroneStatus, LamportClock, Occurrence, OccurrenceStatus, env_int, log, split_csv


BROKER_ID = env_int("BROKER_ID", 1)
PORT = env_int("PORT", 8000)
BROKER_URL = os.getenv("BROKER_URL", f"http://broker-{BROKER_ID}:{PORT}")
PEERS = split_csv(os.getenv("PEERS"))
HEARTBEAT_INTERVAL = env_int("HEARTBEAT_INTERVAL", 2)
HEARTBEAT_TIMEOUT = env_int("HEARTBEAT_TIMEOUT", 6)
DISPATCH_INTERVAL = env_int("DISPATCH_INTERVAL", 1)
DISPATCH_START_DELAY = env_int("DISPATCH_START_DELAY", 5)

clock = LamportClock()
state_lock = threading.RLock()
occurrences: dict[str, Occurrence] = {}
drones: dict[str, DroneInfo] = {}
peer_status: dict[int, dict[str, Any]] = {}
known_brokers: dict[int, str] = {BROKER_ID: BROKER_URL}
stop_event = threading.Event()
started_at = time.time()


class OccurrenceIn(BaseModel):
    sector_id: int
    severity: int
    sensor_id: str
    description: str


class DroneRegistration(BaseModel):
    drone_id: str
    callback_url: str


class PeerState(BaseModel):
    broker_id: int
    broker_url: str
    lamport_ts: int
    occurrences: list[dict[str, Any]]
    drones: list[dict[str, Any]]


class MissionDone(BaseModel):
    occurrence_id: str
    drone_id: str


def peer_id_from_url(url: str) -> int | None:
    name = url.rstrip("/").split("//")[-1].split(":")[0]
    if name.startswith("broker-"):
        try:
            return int(name.split("-", 1)[1])
        except ValueError:
            return None
    return None


def active_broker_ids() -> list[int]:
    now = time.time()
    ids = [BROKER_ID]
    with state_lock:
        for peer_id, status in peer_status.items():
            if status.get("alive") and now - status.get("last_seen", 0) <= HEARTBEAT_TIMEOUT:
                ids.append(peer_id)
    return sorted(set(ids))


def coordinator_id() -> int:
    return min(active_broker_ids())


def owner_for_key(key: str) -> int:
    ids = active_broker_ids()
    if not ids:
        return BROKER_ID
    return ids[sum(ord(char) for char in key) % len(ids)]


def merge_occurrence(data: dict[str, Any]) -> None:
    incoming = Occurrence.from_dict(data)
    clock.update(incoming.lamport_ts)
    with state_lock:
        current = occurrences.get(incoming.occurrence_id)
        if current is None or incoming.lamport_ts >= current.lamport_ts:
            occurrences[incoming.occurrence_id] = incoming


def merge_drone(data: dict[str, Any]) -> None:
    incoming = DroneInfo.from_dict(data)
    with state_lock:
        current = drones.get(incoming.drone_id)
        if current is None or incoming.last_seen >= current.last_seen:
            drones[incoming.drone_id] = incoming


def snapshot_state() -> dict[str, Any]:
    with state_lock:
        return {
            "broker_id": BROKER_ID,
            "broker_url": BROKER_URL,
            "lamport_ts": clock.tick(),
            "occurrences": [item.to_dict() for item in occurrences.values()],
            "drones": [item.to_dict() for item in drones.values()],
        }


def replicate_state() -> None:
    payload = snapshot_state()
    for peer in PEERS:
        try:
            requests.post(f"{peer}/peer/state", json=payload, timeout=1.5)
        except requests.RequestException:
            continue


def mark_peer_alive(peer_id: int, peer_url: str) -> None:
    with state_lock:
        known_brokers[peer_id] = peer_url
        previous = peer_status.get(peer_id, {})
        if not previous.get("alive"):
            log(f"broker-{BROKER_ID}", f"heartbeat: broker {peer_id} ativo em {peer_url}")
        peer_status[peer_id] = {"alive": True, "last_seen": time.time(), "url": peer_url}


def mark_peer_down(peer_id: int) -> None:
    with state_lock:
        previous = peer_status.get(peer_id, {})
        if previous.get("alive", True):
            log(f"broker-{BROKER_ID}", f"falha detectada: broker {peer_id} sem heartbeat")
        peer_status[peer_id] = {**previous, "alive": False, "last_seen": previous.get("last_seen", 0)}
        for item in occurrences.values():
            if item.status == OccurrenceStatus.PENDING.value and item.origin_broker_id == peer_id:
                log(
                    f"broker-{BROKER_ID}",
                    f"redistribuindo ocorrencia pendente {item.occurrence_id} do broker {peer_id}",
                )


def heartbeat_loop() -> None:
    for peer in PEERS:
        peer_id = peer_id_from_url(peer)
        if peer_id is not None:
            with state_lock:
                peer_status.setdefault(peer_id, {"alive": False, "last_seen": 0, "url": peer})
                known_brokers[peer_id] = peer

    while not stop_event.is_set():
        for peer in PEERS:
            try:
                response = requests.get(f"{peer}/heartbeat", timeout=1.5)
                response.raise_for_status()
                data = response.json()
                mark_peer_alive(int(data["broker_id"]), data["broker_url"])
            except requests.RequestException:
                peer_id = peer_id_from_url(peer)
                if peer_id is not None:
                    last_seen = peer_status.get(peer_id, {}).get("last_seen", 0)
                    if time.time() - last_seen > HEARTBEAT_TIMEOUT:
                        mark_peer_down(peer_id)
        replicate_state()
        time.sleep(HEARTBEAT_INTERVAL)


def dispatch_loop() -> None:
    while not stop_event.is_set():
        try:
            dispatch_once()
        except Exception as exc:
            log(f"broker-{BROKER_ID}", f"erro no despachante: {exc}")
        time.sleep(DISPATCH_INTERVAL)


def dispatch_once() -> None:
    if time.time() - started_at < DISPATCH_START_DELAY:
        return
    if coordinator_id() != BROKER_ID:
        return

    with state_lock:
        pending = sorted(
            [item for item in occurrences.values() if item.status == OccurrenceStatus.PENDING.value],
            key=lambda item: item.ordering_key,
        )
        available = sorted(
            [
                item
                for item in drones.values()
                if item.status == DroneStatus.AVAILABLE.value and owner_for_key(item.drone_id) in active_broker_ids()
            ],
            key=lambda item: item.drone_id,
        )

        for occurrence in pending:
            if not available:
                break
            drone = available.pop(0)
            occurrence.status = OccurrenceStatus.ASSIGNED.value
            occurrence.assigned_drone_id = drone.drone_id
            occurrence.lamport_ts = clock.tick()
            drone.status = DroneStatus.RESERVED.value
            drone.assigned_occurrence_id = occurrence.occurrence_id
            drone.broker_id = BROKER_ID
            drone.last_seen = time.time()
            log(
                f"broker-{BROKER_ID}",
                "reserva: "
                f"drone={drone.drone_id} ocorrencia={occurrence.occurrence_id} "
                f"prioridade={occurrence.severity} ts={occurrence.lamport_ts}",
            )
            threading.Thread(target=notify_drone, args=(drone, occurrence), daemon=True).start()

    replicate_state()


def notify_drone(drone: DroneInfo, occurrence: Occurrence) -> None:
    payload = {"drone_id": drone.drone_id, "occurrence": occurrence.to_dict(), "broker_url": BROKER_URL}
    try:
        response = requests.post(f"{drone.callback_url}/mission", json=payload, timeout=3)
        response.raise_for_status()
        result = response.json()
        if result.get("status") != "accepted":
            raise requests.RequestException(f"drone refused mission: {result}")
        with state_lock:
            tracked = drones.get(drone.drone_id)
            if tracked and tracked.assigned_occurrence_id == occurrence.occurrence_id:
                tracked.status = DroneStatus.BUSY.value
                tracked.last_seen = time.time()
        log(f"broker-{BROKER_ID}", f"missao enviada para {drone.drone_id}")
    except requests.RequestException:
        with state_lock:
            tracked_drone = drones.get(drone.drone_id)
            tracked_occurrence = occurrences.get(occurrence.occurrence_id)
            if tracked_drone:
                tracked_drone.status = DroneStatus.OFFLINE.value
                tracked_drone.assigned_occurrence_id = None
            if tracked_occurrence and tracked_occurrence.status != OccurrenceStatus.DONE.value:
                tracked_occurrence.status = OccurrenceStatus.PENDING.value
                tracked_occurrence.assigned_drone_id = None
        log(f"broker-{BROKER_ID}", f"drone {drone.drone_id} indisponivel, liberando ocorrencia")
    replicate_state()


@asynccontextmanager
async def lifespan(_: FastAPI):
    log(f"broker-{BROKER_ID}", f"iniciado em {BROKER_URL}; peers={PEERS}")
    heartbeat = threading.Thread(target=heartbeat_loop, daemon=True)
    dispatcher = threading.Thread(target=dispatch_loop, daemon=True)
    heartbeat.start()
    dispatcher.start()
    yield
    stop_event.set()


app = FastAPI(title=f"Broker {BROKER_ID}", lifespan=lifespan)


@app.get("/heartbeat")
def heartbeat() -> dict[str, Any]:
    return {"broker_id": BROKER_ID, "broker_url": BROKER_URL, "lamport_ts": clock.tick()}


@app.post("/occurrences")
def create_occurrence(data: OccurrenceIn) -> dict[str, Any]:
    ts = clock.tick()
    occurrence = Occurrence(
        occurrence_id=f"occ-{BROKER_ID}-{ts}-{int(time.time() * 1000)}",
        origin_broker_id=BROKER_ID,
        sector_id=data.sector_id,
        severity=max(1, min(5, data.severity)),
        lamport_ts=ts,
        broker_id=BROKER_ID,
        sensor_id=data.sensor_id,
        description=data.description,
    )
    with state_lock:
        occurrences[occurrence.occurrence_id] = occurrence
    log(
        f"broker-{BROKER_ID}",
        f"ocorrencia criada {occurrence.occurrence_id}: prioridade={occurrence.severity} ts={ts}",
    )
    replicate_state()
    return occurrence.to_dict()


@app.post("/drones/register")
def register_drone(data: DroneRegistration) -> dict[str, Any]:
    with state_lock:
        drone = drones.get(data.drone_id)
        if drone and drone.status in {DroneStatus.RESERVED.value, DroneStatus.BUSY.value}:
            drone.callback_url = data.callback_url
            drone.last_seen = time.time()
            log(f"broker-{BROKER_ID}", f"drone {data.drone_id} manteve estado {drone.status}")
            return {"status": drone.status, "broker_id": BROKER_ID}
        drone = DroneInfo(
            drone_id=data.drone_id,
            callback_url=data.callback_url,
            broker_id=BROKER_ID,
            status=DroneStatus.AVAILABLE.value,
        )
        drones[data.drone_id] = drone
    log(f"broker-{BROKER_ID}", f"drone registrado {data.drone_id} em {data.callback_url}")
    replicate_state()
    return {"status": "registered", "broker_id": BROKER_ID}


@app.post("/drones/available")
def drone_available(data: DroneRegistration) -> dict[str, Any]:
    with state_lock:
        drone = drones.get(data.drone_id) or DroneInfo(data.drone_id, data.callback_url)
        drone.callback_url = data.callback_url
        drone.status = DroneStatus.AVAILABLE.value
        drone.assigned_occurrence_id = None
        drone.last_seen = time.time()
        drone.broker_id = BROKER_ID
        drones[data.drone_id] = drone
    log(f"broker-{BROKER_ID}", f"drone disponivel {data.drone_id}")
    replicate_state()
    return {"status": "available"}


@app.post("/missions/done")
def mission_done(data: MissionDone) -> dict[str, Any]:
    with state_lock:
        occurrence = occurrences.get(data.occurrence_id)
        drone = drones.get(data.drone_id)
        if occurrence is None:
            raise HTTPException(status_code=404, detail="occurrence not found")
        occurrence.status = OccurrenceStatus.DONE.value
        occurrence.assigned_drone_id = data.drone_id
        occurrence.lamport_ts = clock.tick()
        if drone:
            drone.status = DroneStatus.AVAILABLE.value
            drone.assigned_occurrence_id = None
            drone.last_seen = time.time()
    log(f"broker-{BROKER_ID}", f"missao concluida: {data.occurrence_id} por {data.drone_id}")
    replicate_state()
    return {"status": "done"}


@app.post("/peer/state")
def receive_peer_state(data: PeerState) -> dict[str, Any]:
    mark_peer_alive(data.broker_id, data.broker_url)
    clock.update(data.lamport_ts)
    for occurrence in data.occurrences:
        merge_occurrence(occurrence)
    for drone in data.drones:
        merge_drone(drone)
    return {"status": "merged", "broker_id": BROKER_ID, "coordinator_id": coordinator_id()}


@app.get("/state")
def state() -> dict[str, Any]:
    with state_lock:
        pending_order = sorted(
            [item for item in occurrences.values() if item.status == OccurrenceStatus.PENDING.value],
            key=lambda item: item.ordering_key,
        )
        return {
            "broker_id": BROKER_ID,
            "broker_url": BROKER_URL,
            "coordinator_id": coordinator_id(),
            "active_brokers": active_broker_ids(),
            "known_brokers": known_brokers,
            "peer_status": peer_status,
            "pending_queue": [item.to_dict() for item in pending_order],
            "occurrences": [item.to_dict() for item in occurrences.values()],
            "drones": [item.to_dict() for item in drones.values()],
        }
