from __future__ import annotations

import os
import random
import threading
import time
from contextlib import asynccontextmanager
from typing import Any

import requests
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

from common import build_id, env_int, log, split_csv


DRONE_ID = os.getenv("DRONE_ID", build_id("drone"))
PORT = env_int("PORT", 9000)
DRONE_URL = os.getenv("DRONE_URL", f"http://{DRONE_ID}:{PORT}")
BROKERS = split_csv(os.getenv("BROKERS"))
REGISTER_INTERVAL = env_int("REGISTER_INTERVAL", 4)
MISSION_MIN_SECONDS = env_int("MISSION_MIN_SECONDS", 4)
MISSION_MAX_SECONDS = env_int("MISSION_MAX_SECONDS", 9)

stop_event = threading.Event()
current_broker: str | None = None
current_mission: str | None = None
state_lock = threading.RLock()


class MissionIn(BaseModel):
    drone_id: str
    occurrence: dict[str, Any]
    broker_url: str


def broker_candidates() -> list[str]:
    candidates = BROKERS[:]
    random.shuffle(candidates)
    return candidates


def register_as_available() -> bool:
    global current_broker
    payload = {"drone_id": DRONE_ID, "callback_url": DRONE_URL}
    for broker in broker_candidates():
        try:
            response = requests.post(f"{broker}/drones/register", json=payload, timeout=2)
            response.raise_for_status()
            current_broker = broker
            log(DRONE_ID, f"registrado em {broker}")
            return True
        except requests.RequestException:
            continue
    current_broker = None
    return False


def reconnect_loop() -> None:
    while not stop_event.is_set():
        with state_lock:
            busy = current_mission is not None
        if not busy:
            ok = register_as_available()
            if not ok:
                log(DRONE_ID, "nenhum broker ativo para registro; tentando novamente")
        time.sleep(REGISTER_INTERVAL)


def finish_mission(occurrence_id: str, broker_url: str) -> None:
    global current_mission
    duration = random.randint(MISSION_MIN_SECONDS, MISSION_MAX_SECONDS)
    log(DRONE_ID, f"executando missao {occurrence_id} por {duration}s")
    time.sleep(duration)
    payload = {"occurrence_id": occurrence_id, "drone_id": DRONE_ID}

    brokers = [broker_url] + [broker for broker in broker_candidates() if broker != broker_url]
    for broker in brokers:
        try:
            response = requests.post(f"{broker}/missions/done", json=payload, timeout=2)
            response.raise_for_status()
            log(DRONE_ID, f"missao {occurrence_id} concluida e reportada para {broker}")
            break
        except requests.RequestException:
            log(DRONE_ID, f"falha ao reportar conclusao para {broker}; tentando outro broker")

    with state_lock:
        current_mission = None
    register_as_available()


@asynccontextmanager
async def lifespan(_: FastAPI):
    if not BROKERS:
        raise RuntimeError("Configure BROKERS com uma lista CSV de URLs")
    log(DRONE_ID, f"iniciado em {DRONE_URL}; brokers={BROKERS}")
    threading.Thread(target=reconnect_loop, daemon=True).start()
    yield
    stop_event.set()


app = FastAPI(title=DRONE_ID, lifespan=lifespan)


@app.post("/mission")
def receive_mission(data: MissionIn) -> dict[str, str]:
    global current_mission
    occurrence_id = data.occurrence["occurrence_id"]
    with state_lock:
        if current_mission is not None:
            return {"status": "busy", "mission": current_mission}
        current_mission = occurrence_id
    log(DRONE_ID, f"missao recebida {occurrence_id} do broker {data.broker_url}")
    threading.Thread(target=finish_mission, args=(occurrence_id, data.broker_url), daemon=True).start()
    return {"status": "accepted"}


@app.get("/health")
def health() -> dict[str, str | None]:
    with state_lock:
        return {"drone_id": DRONE_ID, "current_broker": current_broker, "current_mission": current_mission}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
