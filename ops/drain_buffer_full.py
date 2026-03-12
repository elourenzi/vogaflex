"""
Full buffer drain: processes ALL unique events in smclick_ingest_buffer.

The standard pipeline function uses LIMIT p_limit*10 scan window which can get
stuck when many duplicate SHA256s block access to later buffer rows.
This script advances through the buffer in chunks by buffer ID range.

Run: python ops/drain_buffer_full.py
"""
import psycopg2, time, os

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgres://postgres:da3d6b7ea6af546c3ddc@72.61.132.195:5454/dadosvogaflex?sslmode=disable"
)

BATCH_INSERT_SQL = """
WITH src AS (
  SELECT
    b.id AS source_buffer_id,
    b.payload_hash AS source_payload_hash,
    b.payload,
    COALESCE(NULLIF(BTRIM(b.event_name), ''), NULLIF(BTRIM(b.payload->>'event'), '')) AS event_name,
    COALESCE(b.event_time, public.smclick_try_timestamptz(b.payload->>'event_time'), b.received_at) AS event_time,
    COALESCE(b.chat_id, public.smclick_try_uuid(b.payload #>> '{infos,chat,id}')) AS chat_id,
    COALESCE(b.message_id::text, NULLIF(BTRIM(b.payload #>> '{infos,message,id}'), '')) AS message_id,
    encode(digest(b.payload::text, 'sha256'), 'hex') AS sha256
  FROM public.smclick_ingest_buffer b
  WHERE b.id BETWEEN %(id_from)s AND %(id_to)s
),
deduped AS (
  SELECT DISTINCT ON (s.sha256) s.*
  FROM src s
  WHERE NOT EXISTS (
    SELECT 1 FROM public.smclick_event_log el WHERE el.payload_sha256 = s.sha256
  )
  ORDER BY s.sha256, s.source_buffer_id
)
INSERT INTO public.smclick_event_log (
  source_buffer_id, source_payload_hash, event_name, event_time, chat_id, message_id, payload, payload_sha256
)
SELECT d.source_buffer_id, d.source_payload_hash, d.event_name, d.event_time,
       d.chat_id, d.message_id, d.payload, d.sha256
FROM deduped d
ON CONFLICT DO NOTHING
RETURNING id
"""

if __name__ == "__main__":
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    cur = conn.cursor()

    # Get buffer ID range
    cur.execute("SELECT MIN(id), MAX(id) FROM smclick_ingest_buffer")
    min_id, max_id = cur.fetchone()
    print(f"Buffer ID range: {min_id} — {max_id}")

    # Get current event_log count
    cur.execute("SELECT COUNT(*) FROM smclick_event_log")
    initial_events = cur.fetchone()[0]
    print(f"Current event_log: {initial_events} entries")

    CHUNK = 50000  # scan 50K buffer rows per chunk
    total_inserted = 0
    chunks = 0
    start = time.time()

    id_cursor = min_id
    while id_cursor <= max_id:
        id_from = id_cursor
        id_to = id_cursor + CHUNK - 1

        cur.execute(BATCH_INSERT_SQL, {"id_from": id_from, "id_to": id_to})
        inserted = cur.rowcount
        conn.commit()

        total_inserted += inserted
        chunks += 1

        if chunks % 5 == 0 or inserted > 0:
            elapsed = time.time() - start
            print(f"Chunk {chunks}: IDs {id_from}-{id_to}, inserted={inserted}, total={total_inserted}, elapsed={elapsed:.0f}s")

        id_cursor += CHUNK

    elapsed = time.time() - start
    print(f"\nInserted {total_inserted} new events into event_log in {elapsed:.0f}s")

    # Now run the canonical pipeline to apply all event_log entries to smclick_chat
    print("\nRunning smclick_sync_from_ingest_buffer to apply events to canonical tables...")
    total_chats = 0
    rounds = 0
    for i in range(500):
        cur.execute("SELECT * FROM public.smclick_sync_from_ingest_buffer(3000)")
        row = cur.fetchone()
        conn.commit()
        if row[0] == 0:
            print(f"Pipeline drained after {rounds} rounds")
            break
        total_chats += row[1]
        rounds += 1
        if rounds % 10 == 0:
            cur.execute("SELECT COUNT(*), MAX(last_event_at) FROM smclick_chat")
            r = cur.fetchone()
            print(f"  Round {rounds}: chats={r[0]}, newest={r[1]}, elapsed={time.time()-start:.0f}s")

    cur.execute("SELECT COUNT(*), MAX(last_event_at), COUNT(CASE WHEN attendant_name IS NOT NULL THEN 1 END) FROM smclick_chat")
    r = cur.fetchone()
    print(f"\nFINAL smclick_chat: {r[0]} chats, newest={r[1]}, com_atendente={r[2]}")

    cur.execute("SELECT COUNT(*) FROM smclick_message")
    print(f"smclick_message: {cur.fetchone()[0]} messages")

    conn.close()
