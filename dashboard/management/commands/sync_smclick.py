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

        elapsed = time.time() - t0
        self.stdout.write(
            self.style.SUCCESS(
                f"Done: {total:,} events processed in {elapsed:.1f}s"
            )
        )
