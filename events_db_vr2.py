import os
import json
import time
import requests
import psycopg2
import asyncio
from datetime import datetime
from psycopg2.extras import execute_values
import lib.commonutility as common

# ---------------- CONFIG & LOGGING ----------------
cwd = os.getcwd()
cfg_path = os.path.join(cwd, 'cfg')
ini_path = os.path.join(cfg_path, 'config.ini')
log_path = os.path.join(cwd, 'logs')

common.validate_path(cfg_path)
common.validate_path(ini_path)
common.validate_path(log_path)

config = common.read_config_ini(ini_path)
logger = common.setup_logger()
logger.info("Starting Instana to BHOM event processing...")

# ---------------- UTILITY FUNCTIONS ----------------
async def create_epoc_time():
    curr_dt = datetime.now()
    timestamp = int(round(curr_dt.timestamp())) * 1000
    return timestamp

def get_db_connection():
    return psycopg2.connect(
        host=config["postgres"]["host"],
        database=config["postgres"]["database"],
        user=config["postgres"]["user"],
        password=config["postgres"]["password"],
        port=config["postgres"]["port"]
    )

def init_db():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS instana_events (
            id SERIAL PRIMARY KEY,
            event_id TEXT UNIQUE,
            event_json JSONB,
            status TEXT DEFAULT 'RECEIVED',
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

# ---------------- CUSTOM MAPPERS ----------------
def severity_level(sev):
    return "CRITICAL" if sev == 5 else "WARNING"

def uppercase_state(state):
    return state.upper()

def priority_from_severity(sev):
    return f"PRIORITY_{sev}"

CUSTOM_FUNCTIONS = {
    "severity_level": severity_level,
    "uppercase_state": uppercase_state,
    "priority_from_severity": priority_from_severity,
}

def insert_nested_key(d, dotted_key, value):
    keys = dotted_key.split(".")
    for key in keys[:-1]:
        d = d.setdefault(key, {})
    d[keys[-1]] = value

def bhom_mapping_reverse_lookup(field):
    reverse_map = {
        "severity": "severity",
        "status": "state",
        "priority": "severity",
        "class_slots.pn_severity": "severity"
    }
    return reverse_map.get(field, field)

def get_nested_value(data, key_path):
    try:
        parts = key_path.replace("]", "").split(".")
        val = data
        for part in parts:
            if "[" in part:
                key, idx = part.split("[")
                val = val[key][int(idx)]
            else:
                val = val[part]
        return val
    except (KeyError, IndexError, TypeError):
        return None

def resolve_bhom_mapping(event_data, mapping_config):
    payload = {}
    for bhom_field, rule in mapping_config.items():
        if rule.startswith("static:"):
            value = rule.split("static:")[1]
        elif rule.startswith("event_data:"):
            keys = rule.split("event_data:")[1].split("|")
            value = next((get_nested_value(event_data, key.strip()) for key in keys if get_nested_value(event_data, key.strip()) is not None), "")
        elif rule.startswith("func:"):
            func_name = rule.split("func:")[1].strip()
            source_key = bhom_mapping_reverse_lookup(bhom_field)
            raw_value = get_nested_value(event_data, source_key)
            value = CUSTOM_FUNCTIONS.get(func_name, lambda x: "")(raw_value)
        else:
            value = ""
        insert_nested_key(payload, bhom_field, value)
    return [payload]

# ---------------- FETCH MODULE ----------------
async def fetch_instana_events():
    headers = {
        'Authorization': f'apiToken {config["instana"]["token"]}',
        'Accept': 'application/json'
    }
    url = config["instana"]["url"]
    epoc_time = await create_epoc_time()
    payload = json.dumps({
        "timeFrame": {
            "windowSize": config['instana']['window_size'],
            "to": epoc_time
        },
    })

    try:
        response = requests.get(url, headers=headers, data=payload)
        response.raise_for_status()
        events = response.json()
        conn = get_db_connection()
        cur = conn.cursor()
        print(f"Fetched {len(events)} events from Instana.")

        open_events = [ev for ev in events if ev["state"] == "open"]
        # for event in open_events:
        #     event_id = event.get("eventId")
        #     cur.execute("""
        #         INSERT INTO instana_events (event_id, event_json, status)
        #         VALUES (%s, %s, 'RECEIVED')
        #         ON CONFLICT (event_id) DO NOTHING;
        #     """, (event_id, json.dumps(event)))
        #     logger.info(f"Fetched and stored event: {event_id}")
        open_events_data = [
            (event.get("eventId"), json.dumps(event), "RECEIVED")
            for event in open_events
        ]

        execute_values(
            cur,
            """
            INSERT INTO instana_events (event_id, event_json, status)
            VALUES %s
            ON CONFLICT (event_id) DO NOTHING;
            """,
            open_events_data
        )
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Fetched and stored {len(open_events_data)} events.")
    except Exception as e:
        logger.error(f"Error fetching events: {e}")

# ---------------- PROCESSING MODULE (BATCHED) ----------------
def process_events_in_batches(events, batch_size=8500):
    bhom_config = dict(config["bhom_event_mapping"])
    bhom_url = config["bhom"]["url"]
    REFRESH_TOKEN_FILE = "cfg/bhom_refresh_token.json"
    REFRESH_API_URL = config["bhom"]["refresh_api_url"]

    refresh_token = common.get_valid_refresh_token(REFRESH_TOKEN_FILE, REFRESH_API_URL)
    headers = {
        "Authorization": f"Bearer {refresh_token}",
        "Content-Type": "application/json"
    }

    conn = get_db_connection()
    cur = conn.cursor()

    for i in range(0, len(events), batch_size):
        batch = events[i:i + batch_size]
        batch_payload = [resolve_bhom_mapping(ev_json, bhom_config)[0] for _, ev_json in batch]
        event_ids = [ev_id for ev_id, _ in batch]

        try:
            response = requests.post(bhom_url, headers=headers, json=batch_payload)
            if response.status_code == 200:
                resp_json = response.json()
                resource_count = len(resp_json.get("resourceId", []))
                if resource_count != len(batch):
                    logger.error(f"BHOM creation count mismatch for batch starting with event {event_ids[0]}: "
                                 f"Sent {len(batch)}, Created {resource_count}")
                    cur.executemany("UPDATE instana_events SET status='FAILED', updated_at=NOW() WHERE event_id=%s;",
                                    [(ev_id,) for ev_id in event_ids])
                else:
                    cur.executemany("UPDATE instana_events SET status='CREATED', updated_at=NOW() WHERE event_id=%s;",
                                    [(ev_id,) for ev_id in event_ids])
                    logger.info(f"Successfully processed batch of {len(batch)} events starting with {event_ids[0]}")
            else:
                logger.error(f"BHOM API Error for batch starting with {event_ids[0]}: {response.status_code}, {response.text}")
                cur.executemany("UPDATE instana_events SET status='FAILED', updated_at=NOW() WHERE event_id=%s;",
                                [(ev_id,) for ev_id in event_ids])
        except Exception as e:
            logger.error(f"Exception while processing batch starting with {event_ids[0]}: {e}")
            cur.executemany("UPDATE instana_events SET status='FAILED', updated_at=NOW() WHERE event_id=%s;",
                            [(ev_id,) for ev_id in event_ids])
        finally:
            conn.commit()

    cur.close()
    conn.close()

def process_all_events():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT event_id, event_json FROM instana_events WHERE status in ('RECEIVED','FAILED');")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if rows:
        process_events_in_batches(rows)

# ---------------- MAIN ----------------
async def main():
    init_db()
    logger.info("Starting fetch and processing cycle.")
    await fetch_instana_events()
    process_all_events()
    logger.info("Cycle complete.")

if __name__ == "__main__":
    asyncio.run(main())
