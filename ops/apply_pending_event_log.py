"""
Apply pending smclick_event_log entries (applied_at IS NULL) to canonical tables.

The drain_buffer_full.py script inserted events into smclick_event_log but the
pipeline function (smclick_sync_from_ingest_buffer) cannot pick them up because it
reads from smclick_ingest_buffer directly (not from event_log).

This script creates a helper function smclick_apply_pending_event_log() that reads
from event_log WHERE applied_at IS NULL and applies the same upsert logic.

Run: python ops/apply_pending_event_log.py
"""
import psycopg2, time, os

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgres://postgres:da3d6b7ea6af546c3ddc@72.61.132.195:5454/dadosvogaflex?sslmode=disable"
)

CREATE_FUNCTION_SQL = r"""
CREATE OR REPLACE FUNCTION public.smclick_apply_pending_event_log(p_limit integer DEFAULT 3000)
RETURNS TABLE(events_applied integer, chats_upserted integer, messages_upserted integer, attendants_upserted integer, segments_upserted integer)
LANGUAGE plpgsql AS $function$
DECLARE
  v_events integer := 0;
  v_chats  integer := 0;
  v_msgs   integer := 0;
  v_att    integer := 0;
  v_seg    integer := 0;
BEGIN
  -- Read pending event_log entries (payload already stored from drain_buffer_full.py)
  CREATE TEMP TABLE tmp_pending_evt ON COMMIT DROP AS
  SELECT
    el.id,
    el.source_buffer_id,
    el.event_name,
    el.event_time,
    el.chat_id,
    el.message_id,
    el.payload
  FROM public.smclick_event_log el
  WHERE el.applied_at IS NULL
    AND el.apply_error IS NULL
    AND el.chat_id IS NOT NULL
    AND el.payload IS NOT NULL
  ORDER BY el.id
  LIMIT p_limit;

  SELECT COUNT(*) INTO v_events FROM tmp_pending_evt;
  IF v_events = 0 THEN
    RETURN QUERY SELECT 0, 0, 0, 0, 0;
    RETURN;
  END IF;

  -- Upsert smclick_chat (latest event per chat)
  INSERT INTO public.smclick_chat (
    chat_id, protocol, status, flow, department_name, current_stage, finish_reason,
    contact_name, contact_phone, attendant_name, attendant_email,
    budget_value, order_value, product, loss_reason,
    chat_created_at, chat_updated_at, last_event_at, last_message_id, last_payload, refreshed_at
  )
  SELECT
    e.chat_id,
    CASE WHEN NULLIF(BTRIM(e.payload #>> '{infos,chat,protocol}'), '') ~ '^[0-9]+$'
      THEN NULLIF(BTRIM(e.payload #>> '{infos,chat,protocol}'), '')::bigint ELSE NULL END,
    NULLIF(BTRIM(e.payload #>> '{infos,chat,status}'), ''),
    NULLIF(BTRIM(e.payload #>> '{infos,chat,flow}'), ''),
    NULLIF(BTRIM(e.payload #>> '{infos,chat,department,name}'), ''),
    COALESCE(NULLIF(BTRIM(e.payload #>> '{infos,chat,crm_column,name}'), ''), seg_stage.content_text),
    NULLIF(BTRIM(e.payload #>> '{infos,chat,finish_reason,reason}'), ''),
    NULLIF(BTRIM(e.payload #>> '{infos,chat,contact,name}'), ''),
    NULLIF(BTRIM(e.payload #>> '{infos,chat,contact,telephone}'), ''),
    COALESCE(
      (SELECT NULLIF(BTRIM(a->>'name'), '') FROM jsonb_array_elements(CASE WHEN jsonb_typeof(COALESCE(e.payload #> '{infos,chat,attendant}', '[]'::jsonb)) = 'array' THEN COALESCE(e.payload #> '{infos,chat,attendant}', '[]'::jsonb) ELSE '[]'::jsonb END) a WHERE LOWER(COALESCE(a->>'principal', '')) IN ('true', 't', '1') LIMIT 1),
      (SELECT NULLIF(BTRIM(a->>'name'), '') FROM jsonb_array_elements(CASE WHEN jsonb_typeof(COALESCE(e.payload #> '{infos,chat,attendant}', '[]'::jsonb)) = 'array' THEN COALESCE(e.payload #> '{infos,chat,attendant}', '[]'::jsonb) ELSE '[]'::jsonb END) a LIMIT 1)
    ),
    COALESCE(
      (SELECT NULLIF(BTRIM(a->>'email'), '') FROM jsonb_array_elements(CASE WHEN jsonb_typeof(COALESCE(e.payload #> '{infos,chat,attendant}', '[]'::jsonb)) = 'array' THEN COALESCE(e.payload #> '{infos,chat,attendant}', '[]'::jsonb) ELSE '[]'::jsonb END) a WHERE LOWER(COALESCE(a->>'principal', '')) IN ('true', 't', '1') LIMIT 1),
      (SELECT NULLIF(BTRIM(a->>'email'), '') FROM jsonb_array_elements(CASE WHEN jsonb_typeof(COALESCE(e.payload #> '{infos,chat,attendant}', '[]'::jsonb)) = 'array' THEN COALESCE(e.payload #> '{infos,chat,attendant}', '[]'::jsonb) ELSE '[]'::jsonb END) a LIMIT 1)
    ),
    seg_budget.content_numeric, seg_order.content_numeric, seg_product.content_text, seg_loss.content_text,
    public.smclick_try_timestamptz(e.payload #>> '{infos,chat,created_at}'),
    public.smclick_try_timestamptz(e.payload #>> '{infos,chat,updated_at}'),
    e.event_time,
    COALESCE(NULLIF(BTRIM(e.payload #>> '{infos,message,id}'), ''), NULLIF(BTRIM(e.payload #>> '{infos,chat,last_message,id}'), '')),
    e.payload #> '{infos,chat}',
    NOW()
  FROM (
    SELECT DISTINCT ON (t.chat_id) t.*
    FROM tmp_pending_evt t WHERE t.chat_id IS NOT NULL
    ORDER BY t.chat_id, t.event_time DESC NULLS LAST, t.source_buffer_id DESC
  ) e
  LEFT JOIN LATERAL (
    SELECT NULLIF(BTRIM(seg->>'content'), '') AS content_text, public.smclick_parse_numeric(seg->>'content') AS content_numeric
    FROM jsonb_array_elements(CASE WHEN jsonb_typeof(COALESCE(e.payload #> '{infos,chat,contact,segmentation_fields}', '[]'::jsonb)) = 'array' THEN COALESCE(e.payload #> '{infos,chat,contact,segmentation_fields}', '[]'::jsonb) ELSE '[]'::jsonb END) seg
    WHERE unaccent(lower(COALESCE(seg->>'name', ''))) LIKE '%orcamento%' LIMIT 1
  ) seg_budget ON TRUE
  LEFT JOIN LATERAL (
    SELECT public.smclick_parse_numeric(seg->>'content') AS content_numeric
    FROM jsonb_array_elements(CASE WHEN jsonb_typeof(COALESCE(e.payload #> '{infos,chat,contact,segmentation_fields}', '[]'::jsonb)) = 'array' THEN COALESCE(e.payload #> '{infos,chat,contact,segmentation_fields}', '[]'::jsonb) ELSE '[]'::jsonb END) seg
    WHERE unaccent(lower(COALESCE(seg->>'name', ''))) LIKE '%pedido%' LIMIT 1
  ) seg_order ON TRUE
  LEFT JOIN LATERAL (
    SELECT NULLIF(BTRIM(seg->>'content'), '') AS content_text
    FROM jsonb_array_elements(CASE WHEN jsonb_typeof(COALESCE(e.payload #> '{infos,chat,contact,segmentation_fields}', '[]'::jsonb)) = 'array' THEN COALESCE(e.payload #> '{infos,chat,contact,segmentation_fields}', '[]'::jsonb) ELSE '[]'::jsonb END) seg
    WHERE unaccent(lower(COALESCE(seg->>'name', ''))) LIKE '%produto%' LIMIT 1
  ) seg_product ON TRUE
  LEFT JOIN LATERAL (
    SELECT NULLIF(BTRIM(seg->>'content'), '') AS content_text
    FROM jsonb_array_elements(CASE WHEN jsonb_typeof(COALESCE(e.payload #> '{infos,chat,contact,segmentation_fields}', '[]'::jsonb)) = 'array' THEN COALESCE(e.payload #> '{infos,chat,contact,segmentation_fields}', '[]'::jsonb) ELSE '[]'::jsonb END) seg
    WHERE unaccent(lower(COALESCE(seg->>'name', ''))) LIKE '%motivo de perda%' LIMIT 1
  ) seg_loss ON TRUE
  LEFT JOIN LATERAL (
    SELECT NULLIF(BTRIM(seg->>'content'), '') AS content_text
    FROM jsonb_array_elements(CASE WHEN jsonb_typeof(COALESCE(e.payload #> '{infos,chat,contact,segmentation_fields}', '[]'::jsonb)) = 'array' THEN COALESCE(e.payload #> '{infos,chat,contact,segmentation_fields}', '[]'::jsonb) ELSE '[]'::jsonb END) seg
    WHERE unaccent(lower(COALESCE(seg->>'name', ''))) LIKE '%etapa%'
      AND unaccent(lower(COALESCE(seg->>'name', ''))) LIKE '%funil%'
    LIMIT 1
  ) seg_stage ON TRUE
  WHERE e.chat_id IS NOT NULL
  ON CONFLICT (chat_id) DO UPDATE SET
    protocol        = COALESCE(public.smclick_chat.protocol, EXCLUDED.protocol),
    status          = COALESCE(EXCLUDED.status, public.smclick_chat.status),
    flow            = COALESCE(EXCLUDED.flow, public.smclick_chat.flow),
    department_name = COALESCE(EXCLUDED.department_name, public.smclick_chat.department_name),
    current_stage   = COALESCE(EXCLUDED.current_stage, public.smclick_chat.current_stage),
    finish_reason   = COALESCE(EXCLUDED.finish_reason, public.smclick_chat.finish_reason),
    contact_name    = COALESCE(EXCLUDED.contact_name, public.smclick_chat.contact_name),
    contact_phone   = COALESCE(EXCLUDED.contact_phone, public.smclick_chat.contact_phone),
    attendant_name  = COALESCE(EXCLUDED.attendant_name, public.smclick_chat.attendant_name),
    attendant_email = COALESCE(EXCLUDED.attendant_email, public.smclick_chat.attendant_email),
    budget_value    = COALESCE(EXCLUDED.budget_value, public.smclick_chat.budget_value),
    order_value     = COALESCE(EXCLUDED.order_value, public.smclick_chat.order_value),
    product         = COALESCE(EXCLUDED.product, public.smclick_chat.product),
    loss_reason     = COALESCE(EXCLUDED.loss_reason, public.smclick_chat.loss_reason),
    chat_created_at = COALESCE(public.smclick_chat.chat_created_at, EXCLUDED.chat_created_at),
    chat_updated_at = CASE
      WHEN public.smclick_chat.chat_updated_at IS NULL THEN EXCLUDED.chat_updated_at
      WHEN EXCLUDED.chat_updated_at IS NULL THEN public.smclick_chat.chat_updated_at
      ELSE GREATEST(public.smclick_chat.chat_updated_at, EXCLUDED.chat_updated_at) END,
    last_event_at   = CASE
      WHEN public.smclick_chat.last_event_at IS NULL THEN EXCLUDED.last_event_at
      WHEN EXCLUDED.last_event_at IS NULL THEN public.smclick_chat.last_event_at
      ELSE GREATEST(public.smclick_chat.last_event_at, EXCLUDED.last_event_at) END,
    last_message_id = COALESCE(EXCLUDED.last_message_id, public.smclick_chat.last_message_id),
    last_payload    = EXCLUDED.last_payload,
    refreshed_at    = NOW();
  GET DIAGNOSTICS v_chats = ROW_COUNT;

  -- Upsert smclick_message
  INSERT INTO public.smclick_message (
    chat_id, message_id, event_time, message_type, message_stage, from_me, message_status,
    content_text, content_original_text, sent_at, sent_by_name, sent_by_email, fail_reason, payload, last_seen_at
  )
  SELECT e.chat_id,
    COALESCE(NULLIF(BTRIM(e.payload #>> '{infos,message,id}'), ''), md5(COALESCE(e.chat_id::text,'') || '|' || COALESCE(e.payload #>> '{infos,message,sent_at}','') || '|' || COALESCE(e.payload #>> '{infos,message,content,text}',''))),
    COALESCE(public.smclick_try_timestamptz(e.payload #>> '{infos,message,sent_at}'), public.smclick_try_timestamptz(e.payload #>> '{infos,message,created_at}'), e.event_time),
    NULLIF(BTRIM(e.payload #>> '{infos,message,type}'), ''),
    NULLIF(BTRIM(e.payload #>> '{infos,message,stage}'), ''),
    CASE WHEN LOWER(COALESCE(e.payload #>> '{infos,message,from_me}', '')) IN ('true','t','1') THEN TRUE
         WHEN LOWER(COALESCE(e.payload #>> '{infos,message,from_me}', '')) IN ('false','f','0') THEN FALSE ELSE NULL END,
    CASE WHEN LOWER(COALESCE(e.payload #>> '{infos,message,status}', '')) IN ('true','t','1') THEN TRUE
         WHEN LOWER(COALESCE(e.payload #>> '{infos,message,status}', '')) IN ('false','f','0') THEN FALSE ELSE NULL END,
    NULLIF(BTRIM(e.payload #>> '{infos,message,content,text}'), ''),
    NULLIF(BTRIM(e.payload #>> '{infos,message,content,original_text}'), ''),
    public.smclick_try_timestamptz(e.payload #>> '{infos,message,sent_at}'),
    NULLIF(BTRIM(e.payload #>> '{infos,message,sent_by,name}'), ''),
    NULLIF(BTRIM(e.payload #>> '{infos,message,sent_by,email}'), ''),
    NULLIF(BTRIM(e.payload #>> '{infos,message,fail_reason}'), ''),
    e.payload #> '{infos,message}',
    COALESCE(e.event_time, NOW())
  FROM (
    SELECT DISTINCT ON (t.chat_id, COALESCE(NULLIF(BTRIM(t.payload #>> '{infos,message,id}'),''), md5(COALESCE(t.chat_id::text,'') || '|' || COALESCE(t.payload #>> '{infos,message,sent_at}','') || '|' || COALESCE(t.payload #>> '{infos,message,content,text}','')))) t.*
    FROM tmp_pending_evt t
    WHERE t.chat_id IS NOT NULL AND t.payload #> '{infos,message}' IS NOT NULL
    ORDER BY t.chat_id,
      COALESCE(NULLIF(BTRIM(t.payload #>> '{infos,message,id}'),''), md5(COALESCE(t.chat_id::text,'') || '|' || COALESCE(t.payload #>> '{infos,message,sent_at}','') || '|' || COALESCE(t.payload #>> '{infos,message,content,text}',''))),
      t.event_time DESC NULLS LAST, t.source_buffer_id DESC
  ) e
  ON CONFLICT (chat_id, message_id) DO UPDATE SET
    event_time             = COALESCE(EXCLUDED.event_time, public.smclick_message.event_time),
    message_type           = COALESCE(EXCLUDED.message_type, public.smclick_message.message_type),
    message_stage          = COALESCE(EXCLUDED.message_stage, public.smclick_message.message_stage),
    from_me                = COALESCE(EXCLUDED.from_me, public.smclick_message.from_me),
    message_status         = COALESCE(EXCLUDED.message_status, public.smclick_message.message_status),
    content_text           = COALESCE(EXCLUDED.content_text, public.smclick_message.content_text),
    content_original_text  = COALESCE(EXCLUDED.content_original_text, public.smclick_message.content_original_text),
    sent_at                = COALESCE(EXCLUDED.sent_at, public.smclick_message.sent_at),
    sent_by_name           = COALESCE(EXCLUDED.sent_by_name, public.smclick_message.sent_by_name),
    sent_by_email          = COALESCE(EXCLUDED.sent_by_email, public.smclick_message.sent_by_email),
    fail_reason            = COALESCE(EXCLUDED.fail_reason, public.smclick_message.fail_reason),
    payload                = COALESCE(EXCLUDED.payload, public.smclick_message.payload),
    last_seen_at           = EXCLUDED.last_seen_at;
  GET DIAGNOSTICS v_msgs = ROW_COUNT;

  -- Upsert smclick_chat_attendant
  INSERT INTO public.smclick_chat_attendant (chat_id, attendant_key, attendant_id, attendant_name, attendant_email, principal, last_seen_at, raw)
  SELECT e.chat_id, e.attendant_key, e.attendant_id, e.attendant_name, e.attendant_email, e.principal, COALESCE(e.event_time, NOW()), e.attendant_raw
  FROM (
    SELECT DISTINCT ON (x.chat_id, x.attendant_key) x.*
    FROM (
      SELECT t.chat_id, t.event_time, t.source_buffer_id,
        COALESCE(NULLIF(BTRIM(a->>'id'),''), md5(COALESCE(a->>'email','') || '|' || COALESCE(a->>'name',''))) AS attendant_key,
        public.smclick_try_uuid(a->>'id') AS attendant_id,
        NULLIF(BTRIM(a->>'name'),'') AS attendant_name, NULLIF(BTRIM(a->>'email'),'') AS attendant_email,
        CASE WHEN LOWER(COALESCE(a->>'principal','')) IN ('true','t','1') THEN TRUE ELSE FALSE END AS principal,
        a AS attendant_raw
      FROM tmp_pending_evt t
      CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(COALESCE(t.payload #> '{infos,chat,attendant}','[]'::jsonb)) = 'array' THEN COALESCE(t.payload #> '{infos,chat,attendant}','[]'::jsonb) ELSE '[]'::jsonb END) a
      WHERE t.chat_id IS NOT NULL
    ) x
    ORDER BY x.chat_id, x.attendant_key, x.event_time DESC NULLS LAST, x.source_buffer_id DESC
  ) e
  ON CONFLICT (chat_id, attendant_key) DO UPDATE SET
    attendant_id    = COALESCE(EXCLUDED.attendant_id, public.smclick_chat_attendant.attendant_id),
    attendant_name  = COALESCE(EXCLUDED.attendant_name, public.smclick_chat_attendant.attendant_name),
    attendant_email = COALESCE(EXCLUDED.attendant_email, public.smclick_chat_attendant.attendant_email),
    principal       = COALESCE(EXCLUDED.principal, public.smclick_chat_attendant.principal),
    last_seen_at    = EXCLUDED.last_seen_at,
    raw             = EXCLUDED.raw;
  GET DIAGNOSTICS v_att = ROW_COUNT;

  -- Upsert smclick_chat_segment
  INSERT INTO public.smclick_chat_segment (chat_id, field_key, field_name, field_type, field_position, content_text, content_numeric, last_seen_at, raw)
  SELECT e.chat_id, e.field_key, e.field_name, e.field_type, e.field_position, e.content_text, e.content_numeric, COALESCE(e.event_time, NOW()), e.segment_raw
  FROM (
    SELECT DISTINCT ON (x.chat_id, x.field_key) x.*
    FROM (
      SELECT t.chat_id, t.event_time, t.source_buffer_id,
        COALESCE(NULLIF(BTRIM(s->>'id'),''), md5(s::text)) AS field_key,
        NULLIF(BTRIM(s->>'name'),'') AS field_name, NULLIF(BTRIM(s->>'type'),'') AS field_type,
        CASE WHEN NULLIF(BTRIM(s->>'position'),'') ~ '^[0-9]+$' THEN (s->>'position')::int ELSE NULL END AS field_position,
        NULLIF(BTRIM(s->>'content'),'') AS content_text, public.smclick_parse_numeric(s->>'content') AS content_numeric,
        s AS segment_raw
      FROM tmp_pending_evt t
      CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(COALESCE(t.payload #> '{infos,chat,contact,segmentation_fields}','[]'::jsonb)) = 'array' THEN COALESCE(t.payload #> '{infos,chat,contact,segmentation_fields}','[]'::jsonb) ELSE '[]'::jsonb END) s
      WHERE t.chat_id IS NOT NULL
    ) x
    ORDER BY x.chat_id, x.field_key, x.event_time DESC NULLS LAST, x.source_buffer_id DESC
  ) e
  ON CONFLICT (chat_id, field_key) DO UPDATE SET
    field_name      = COALESCE(EXCLUDED.field_name, public.smclick_chat_segment.field_name),
    field_type      = COALESCE(EXCLUDED.field_type, public.smclick_chat_segment.field_type),
    field_position  = COALESCE(EXCLUDED.field_position, public.smclick_chat_segment.field_position),
    content_text    = COALESCE(EXCLUDED.content_text, public.smclick_chat_segment.content_text),
    content_numeric = COALESCE(EXCLUDED.content_numeric, public.smclick_chat_segment.content_numeric),
    last_seen_at    = EXCLUDED.last_seen_at,
    raw             = EXCLUDED.raw;
  GET DIAGNOSTICS v_seg = ROW_COUNT;

  -- Mark as applied
  UPDATE public.smclick_event_log el
  SET applied_at = NOW(), apply_error = NULL
  WHERE el.id IN (SELECT id FROM tmp_pending_evt);

  RETURN QUERY SELECT v_events, v_chats, v_msgs, v_att, v_seg;
END;
$function$;
"""

if __name__ == "__main__":
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    cur = conn.cursor()

    print("Creating function smclick_apply_pending_event_log()...")
    cur.execute(CREATE_FUNCTION_SQL)
    print("Function created.")

    # Run in loop until drained
    total_events = 0
    total_chats = 0
    total_msgs = 0
    rounds = 0
    start = time.time()

    print("\nApplying pending event_log entries to canonical tables...")
    for i in range(5000):
        cur.execute("SELECT * FROM public.smclick_apply_pending_event_log(3000)")
        row = cur.fetchone()
        events, chats, msgs, att, seg = row

        if events == 0:
            print(f"Done — all pending events applied after {rounds} rounds ({time.time()-start:.0f}s)")
            break

        total_events += events
        total_chats += chats
        total_msgs += msgs
        rounds += 1

        if rounds % 10 == 0 or rounds <= 5:
            elapsed = time.time() - start
            print(f"  Round {rounds:4d}: events={events}, chats_upserted={chats}, msgs={msgs}, "
                  f"total_events={total_events}, elapsed={elapsed:.0f}s")

    elapsed = time.time() - start
    print(f"\nTotal: {total_events} events applied, {rounds} rounds, {elapsed:.0f}s")

    cur.execute("SELECT COUNT(*), MAX(last_event_at), COUNT(CASE WHEN attendant_name IS NOT NULL THEN 1 END) FROM smclick_chat")
    r = cur.fetchone()
    print(f"smclick_chat: {r[0]} chats, newest={r[1]}, com_atendente={r[2]}")

    cur.execute("SELECT COUNT(*) FROM smclick_message")
    print(f"smclick_message: {cur.fetchone()[0]} messages")

    cur.execute("SELECT COUNT(*) FROM smclick_event_log WHERE applied_at IS NULL")
    print(f"smclick_event_log pending: {cur.fetchone()[0]}")

    conn.close()
