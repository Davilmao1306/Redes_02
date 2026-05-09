from __future__ import annotations

import os
import random
import time

import requests

from common import build_id, env_int, log, split_csv


SENSOR_ID = os.getenv("SENSOR_ID", build_id("sensor"))
SECTOR_ID = env_int("SECTOR_ID", 1)
BROKERS = split_csv(os.getenv("BROKERS"))
INTERVAL_MIN = env_int("INTERVAL_MIN", 3)
INTERVAL_MAX = env_int("INTERVAL_MAX", 7)


def choose_broker() -> str | None:
    candidates = BROKERS[:]
    random.shuffle(candidates)
    for broker in candidates:
        try:
            response = requests.get(f"{broker}/heartbeat", timeout=1.5)
            response.raise_for_status()
            return broker
        except requests.RequestException:
            continue
    return None


def main() -> None:
    if not BROKERS:
        raise SystemExit("Configure BROKERS com uma lista CSV de URLs")

    log(SENSOR_ID, f"iniciado no setor {SECTOR_ID}; brokers={BROKERS}")
    while True:
        broker = choose_broker()
        if broker is None:
            log(SENSOR_ID, "nenhum broker ativo encontrado; tentando novamente")
            time.sleep(2)
            continue

        severity = random.randint(1, 5)
        payload = {
            "sector_id": SECTOR_ID,
            "severity": severity,
            "sensor_id": SENSOR_ID,
            "description": f"telemetria anomala no setor {SECTOR_ID}, severidade {severity}",
        }
        try:
            response = requests.post(f"{broker}/occurrences", json=payload, timeout=2)
            response.raise_for_status()
            data = response.json()
            log(SENSOR_ID, f"ocorrencia enviada para {broker}: {data['occurrence_id']}")
        except requests.RequestException:
            log(SENSOR_ID, f"falha ao enviar para {broker}; reconectando")

        time.sleep(random.randint(INTERVAL_MIN, INTERVAL_MAX))


if __name__ == "__main__":
    main()
