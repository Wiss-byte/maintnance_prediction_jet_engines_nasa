import datetime as dt
import json
import os
import random
import time
import uuid

import paho.mqtt.client as mqtt
import psycopg2

from schema import OPS, SENSORS

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
PG_DSN = os.getenv("PG_DSN", "host=localhost dbname=pm user=postgres password=postgres")
N = int(os.getenv("N_ENGINES", "50"))
TICK = float(os.getenv("TICK", "1.0"))
MAX_ENGINES = int(os.getenv("MAX_ENGINES", "0"))
FAULTS = os.getenv("FAULTS", "1") == "1"
RUN = uuid.uuid4().hex[:4]

MODELS = ["CFM-A", "CFM-B", "GE-X"]
SITES = ["Casablanca", "Marrakech", "Tanger", "Agadir"]
OPERATORS = [(f"Operator {i}", f"operator{i}@example.org") for i in range(1, 13)]


def now_iso():
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds")


class Engine:
    def __init__(self, eid):
        self.id, self.cycle = eid, 0
        self.model, self.site = random.choice(MODELS), random.choice(SITES)
        self.op_name, self.op_email = random.choice(OPERATORS)
        self.life = int(min(350, max(120, random.gauss(205, 45))))
        self.k = {s: random.uniform(0.8, 1.2) for s in SENSORS}
        self.bias = {s: random.gauss(0, abs(d) * 0.15) for s, (_, d) in SENSORS.items()}

    def step(self):
        self.cycle += 1
        rec = {"engine_id": self.id, "cycle": self.cycle, "event_ts": now_iso()}
        wear = (self.cycle / self.life) ** 2
        rec.update(op1=round(random.gauss(0, 0.002), 4), op2=round(random.gauss(0, 0.0003), 4), op3=100.0)
        for s, (nom, drift) in SENSORS.items():
            v = nom * (1 + self.bias[s] + drift * self.k[s] * wear + random.gauss(0, abs(drift) * 0.25))
            rec[s] = round(v, 4)
        return rec

    @property
    def done(self):
        return self.cycle >= self.life


def publish(cli, rec):
    out = dict(rec)
    if FAULTS:
        r = random.random()
        if r < 0.004:
            out[random.choice(list(SENSORS))] = None
        elif r < 0.007:
            s = random.choice(list(SENSORS))
            out[s] = round(out[s] * 10, 4)
    payload = json.dumps(out)
    topic = f"engines/{out['engine_id']}/telemetry"
    cli.publish(topic, payload, qos=1)
    if FAULTS and random.random() < 0.005:
        cli.publish(topic, payload, qos=1)


def main():
    cli = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"sim-{RUN}")
    cli.max_inflight_messages_set(1000)
    cli.max_queued_messages_set(0)
    while True:
        try:
            cli.connect(MQTT_HOST, 1883, 60)
            break
        except OSError:
            print("MQTT indisponible, nouvel essai...")
            time.sleep(3)
    cli.loop_start()

    pg = psycopg2.connect(PG_DSN)
    pg.autocommit = True
    cur = pg.cursor()
    created = 0

    def new_engine():
        nonlocal created
        if MAX_ENGINES and created >= MAX_ENGINES:
            return None
        e = Engine(f"SIM-{RUN}-{created:05d}")
        created += 1
        cur.execute(
            "INSERT INTO engines(engine_id, model, site, operator_name, operator_email, status) "
            "VALUES (%s,%s,%s,%s,%s,'RUNNING') ON CONFLICT (engine_id) DO NOTHING",
            (e.id, e.model, e.site, e.op_name, e.op_email),
        )
        return e

    active = [e for e in (new_engine() for _ in range(N)) if e]
    print(f"[sim] moteurs actifs={len(active)} tick={TICK}s faults={FAULTS}")
    sent = 0
    while active:
        t0 = time.time()
        for e in list(active):
            publish(cli, e.step())
            sent += 1
            if e.done:
                cur.execute("UPDATE engines SET status='FAILED', failure_cycle=%s WHERE engine_id=%s", (e.cycle, e.id))
                active.remove(e)
                ne = new_engine()
                if ne:
                    active.append(ne)
        if sent % 500 < len(active) + 1:
            print(f"[sim] {sent} messages, {len(active)} moteurs actifs, {created} créés", flush=True)
        if TICK > 0:
            time.sleep(max(0.0, TICK - (time.time() - t0)))
    time.sleep(2)
    cli.loop_stop()
    print(f"[sim] terminé : {sent} messages")


if __name__ == "__main__":
    main()