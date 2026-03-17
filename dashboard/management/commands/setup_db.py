"""
Django management command: setup_db

Runs all idempotent DDL migrations needed by the app.
Safe to run multiple times — uses IF NOT EXISTS everywhere.

Usage:
  python manage.py setup_db
"""
from django.core.management.base import BaseCommand
from django.db import connection


class Command(BaseCommand):
    help = "Create tables, indexes, and backfill data needed by the app"

    def handle(self, *args, **options):
        with connection.cursor() as cur:
            # ── 1. bot_transfers table ──────────────────────────────
            self.stdout.write("Creating bot_transfers table...")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS public.bot_transfers (
                    chat_id TEXT PRIMARY KEY,
                    transfer_ts TIMESTAMPTZ NOT NULL,
                    source TEXT NOT NULL DEFAULT 'backfill',
                    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)

            # ── 2. Indexes on smclick_chat ──────────────────────────
            self.stdout.write("Creating indexes on smclick_chat...")
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_smclick_chat_created
                    ON public.smclick_chat (chat_created_at)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_smclick_chat_attendant
                    ON public.smclick_chat (attendant_name)
                    WHERE attendant_name IS NOT NULL
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_smclick_chat_status
                    ON public.smclick_chat (status)
            """)

            # ── 3. Indexes on messages ──────────────────────────────
            self.stdout.write("Creating indexes on messages...")
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_messages_chat_ts
                    ON public.messages (chat_id, "timestamp")
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_messages_chat_from_ts
                    ON public.messages (chat_id, from_client, "timestamp")
            """)

            # ── 4. Partial index on smclick_message ─────────────────
            self.stdout.write("Creating index on smclick_message...")
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_smclick_msg_bot_candidates
                    ON public.smclick_message (chat_id, event_time)
                    WHERE from_me = true AND sent_by_name IS NULL
            """)

            # ── 5. Backfill bot_transfers ───────────────────────────
            self.stdout.write("Backfilling bot_transfers...")
            cur.execute("""
                INSERT INTO bot_transfers (chat_id, transfer_ts, source)
                SELECT chat_id, MIN(bot_ts), 'backfill'
                FROM (
                    SELECT m.chat_id, m."timestamp" AS bot_ts
                    FROM messages m
                    WHERE m.from_client = false AND m.content IS NOT NULL
                        AND (
                            m.content ILIKE '%%atendimento ao nosso setor de vendas%%'
                            OR m.content ILIKE '%%atendimento ao nosso time de vendas%%'
                            OR m.content ILIKE '%%encaminhar ao nosso time de vendas%%'
                        )
                    UNION ALL
                    SELECT sm.chat_id::text, sm.event_time AS bot_ts
                    FROM smclick_message sm
                    WHERE sm.from_me = true AND sm.sent_by_name IS NULL
                        AND sm.content_text IS NOT NULL
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
            self.stdout.write(f"  bot_transfers: {bt_count} rows upserted")

            # ── 6. chat_budget_detected table ─────────────────────────
            self.stdout.write("Creating chat_budget_detected table...")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS public.chat_budget_detected (
                    chat_id TEXT PRIMARY KEY,
                    budget_value NUMERIC(18,2) NOT NULL,
                    source TEXT NOT NULL DEFAULT 'backfill',
                    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_chat_budget_detected_value
                    ON public.chat_budget_detected (budget_value)
                    WHERE budget_value > 0
            """)

            # ── 7. Backfill chat_budget_detected from messages ────────
            self.stdout.write("Backfilling chat_budget_detected...")
            cur.execute("""
                INSERT INTO chat_budget_detected (chat_id, budget_value, source)
                SELECT
                    chat_id_txt,
                    MAX(msg_budget),
                    'backfill'
                FROM (
                    -- SmClick messages
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
                    ) AS matches ON TRUE
                    WHERE sm.content_text IS NOT NULL

                    UNION ALL

                    -- Legacy messages table
                    SELECT
                        m.chat_id AS chat_id_txt,
                        NULLIF(
                            REPLACE(REPLACE((matches)[1], '.', ''), ',', '.'),
                            ''
                        )::numeric AS msg_budget
                    FROM messages m
                    CROSS JOIN LATERAL regexp_matches(
                        translate(
                            lower(COALESCE(m.content, '')),
                            'áàâãäéèêëíìîïóòôõöúùûüç',
                            'aaaaaeeeeiiiiooooouuuuc'
                        ),
                        'total\\s*[:\\-]?\\s*r\\$\\s*([0-9\\.]+(?:,[0-9]{2})?)',
                        'g'
                    ) AS matches ON TRUE
                    WHERE m.content IS NOT NULL
                ) _src
                WHERE msg_budget IS NOT NULL
                    AND msg_budget > 0
                    AND msg_budget <= 10000000
                GROUP BY chat_id_txt
                ON CONFLICT (chat_id) DO UPDATE SET
                    budget_value = GREATEST(chat_budget_detected.budget_value, EXCLUDED.budget_value),
                    detected_at = NOW()
            """)
            bd_count = cur.rowcount
            self.stdout.write(f"  chat_budget_detected: {bd_count} rows upserted")

        self.stdout.write(self.style.SUCCESS("setup_db complete"))
