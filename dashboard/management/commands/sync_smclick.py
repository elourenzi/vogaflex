"""
Django management command: sync_smclick

Processes pending events from smclick_ingest_buffer into canonical tables.
Runs smclick_sync_from_ingest_buffer() until drained, then applies any
pre-inserted pending event_log entries via smclick_apply_pending_event_log().

Usage:
  python manage.py sync_smclick           # run once, drain all pending
  python manage.py sync_smclick --batch 1000  # custom batch size

Schedule with OS cron or n8n every 5 minutes:
  */5 * * * * cd /app && python manage.py sync_smclick >> /var/log/smclick_sync.log 2>&1
"""
from django.core.management.base import BaseCommand
from django.db import connection
import time


class Command(BaseCommand):
    help = "Sync smclick_ingest_buffer events to canonical tables"

    def add_arguments(self, parser):
        parser.add_argument("--batch", type=int, default=3000,
                            help="Events per round (default: 3000)")
        parser.add_argument("--max-rounds", type=int, default=500,
                            help="Max rounds before stopping (default: 500)")

    def handle(self, *args, **options):
        batch = options["batch"]
        max_rounds = options["max_rounds"]
        t0 = time.time()
        total = 0

        with connection.cursor() as cur:
            # Phase 1: process new buffer rows
            self.stdout.write("Phase 1: sync from ingest buffer...")
            for i in range(max_rounds):
                cur.execute("SELECT * FROM smclick_sync_from_ingest_buffer(%s)", [batch])
                row = cur.fetchone()
                events = row[0] if row else 0
                if events == 0:
                    break
                total += events
                self.stdout.write(
                    f"  Round {i+1}: events={events}, chats={row[1]}, "
                    f"msgs={row[2]}, elapsed={time.time()-t0:.0f}s"
                )

            # Phase 2: apply any pre-inserted pending event_log entries
            cur.execute(
                "SELECT COUNT(*) FROM smclick_event_log "
                "WHERE applied_at IS NULL AND chat_id IS NOT NULL AND payload IS NOT NULL"
            )
            pending = cur.fetchone()[0]

            if pending > 0:
                self.stdout.write(f"Phase 2: applying {pending:,} pre-inserted pending events...")
                for i in range(max_rounds):
                    cur.execute(
                        "SELECT * FROM smclick_apply_pending_event_log(%s)", [batch]
                    )
                    row = cur.fetchone()
                    events = row[0] if row else 0
                    if events == 0:
                        break
                    total += events
                    self.stdout.write(
                        f"  Round {i+1}: events={events}, chats={row[1]}, "
                        f"msgs={row[2]}, elapsed={time.time()-t0:.0f}s"
                    )

            # Phase 3: refresh bot_transfers for recently touched messages
            self.stdout.write("Phase 3: refreshing bot_transfers...")
            cur.execute("""
                INSERT INTO bot_transfers (chat_id, transfer_ts, source)
                SELECT chat_id, MIN(bot_ts), 'sync'
                FROM (
                  SELECT sm.chat_id::text AS chat_id, sm.event_time AS bot_ts
                  FROM smclick_message sm
                  WHERE sm.from_me = true AND sm.sent_by_name IS NULL
                    AND sm.content_text IS NOT NULL
                    AND sm.last_seen_at >= NOW() - INTERVAL '15 minutes'
                    AND (
                      sm.content_text ILIKE '%%atendimento ao nosso setor de vendas%%'
                      OR sm.content_text ILIKE '%%atendimento ao nosso time de vendas%%'
                      OR sm.content_text ILIKE '%%encaminhar ao nosso time de vendas%%'
                    )
                ) _be
                GROUP BY chat_id
                ON CONFLICT (chat_id) DO UPDATE SET
                  transfer_ts = LEAST(bot_transfers.transfer_ts, EXCLUDED.transfer_ts),
                  detected_at = NOW()
            """)
            bt_count = cur.rowcount
            self.stdout.write(f"  bot_transfers refreshed: {bt_count} rows")

            # Phase 4: refresh chat_budget_detected for recently touched messages
            self.stdout.write("Phase 4: refreshing chat_budget_detected...")
            cur.execute("""
                INSERT INTO chat_budget_detected (chat_id, budget_value, source)
                SELECT
                    chat_id_txt,
                    MAX(msg_budget),
                    'sync'
                FROM (
                    SELECT
                        sm.chat_id::text AS chat_id_txt,
                        NULLIF(
                            REPLACE(REPLACE((matches)[1], '.', ''), ',', '.'),
                            ''
                        )::numeric AS msg_budget
                    FROM smclick_message sm
                    CROSS JOIN LATERAL regexp_matches(
                        translate(
                            lower(COALESCE(sm.content_text, '')),
                            'áàâãäéèêëíìîïóòôõöúùûüç',
                            'aaaaaeeeeiiiiooooouuuuc'
                        ),
                        'total\\s*[:\\-]?\\s*r\\$\\s*([0-9\\.]+(?:,[0-9]{2})?)',
                        'g'
                    ) AS matches
                    WHERE sm.content_text IS NOT NULL
                      AND sm.last_seen_at >= NOW() - INTERVAL '15 minutes'
                ) _src
                WHERE msg_budget IS NOT NULL
                    AND msg_budget > 0
                    AND msg_budget <= 10000000
                GROUP BY chat_id_txt
                ON CONFLICT (chat_id) DO UPDATE SET
                    budget_value = GREATEST(chat_budget_detected.budget_value, EXCLUDED.budget_value),
                    detected_at = NOW()
            """)
            bg_count = cur.rowcount
            self.stdout.write(f"  chat_budget_detected refreshed: {bg_count} rows")

        elapsed = time.time() - t0
        self.stdout.write(
            self.style.SUCCESS(
                f"Done: {total:,} events processed in {elapsed:.1f}s"
            )
        )
