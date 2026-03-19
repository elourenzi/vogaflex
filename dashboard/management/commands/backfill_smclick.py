"""
Django management command: backfill_smclick

Pulls chats (and their messages) from the SmClick REST API for a given
date range and inserts them into smclick_ingest_buffer so the normal
sync_smclick pipeline can process them.

Usage:
  python manage.py backfill_smclick                          # today
  python manage.py backfill_smclick --date-from 2026-03-19   # specific day
  python manage.py backfill_smclick --date-from 2026-03-15 --date-to 2026-03-19
"""
import hashlib
import json
import time

import requests
from django.core.management.base import BaseCommand
from django.db import connection


SMCLICK_API = "https://api.smclick.com.br"


class Command(BaseCommand):
    help = "Backfill chats+messages from SmClick API into ingest buffer"

    def add_arguments(self, parser):
        parser.add_argument("--api-key", type=str, default=None,
                            help="SmClick API key (or set SMCLICK_API_KEY env var)")
        parser.add_argument("--date-from", type=str, default=None,
                            help="Start date YYYY-MM-DD (default: today)")
        parser.add_argument("--date-to", type=str, default=None,
                            help="End date YYYY-MM-DD (default: same as date-from)")
        parser.add_argument("--skip-messages", action="store_true",
                            help="Only backfill chat metadata, skip messages")
        parser.add_argument("--max-pages", type=int, default=500,
                            help="Max pages to fetch (default: 500)")

    def handle(self, *args, **options):
        import os
        api_key = options["api_key"] or os.environ.get("SMCLICK_API_KEY")
        if not api_key:
            self.stderr.write(self.style.ERROR(
                "API key required: --api-key or SMCLICK_API_KEY env var"))
            return

        from datetime import date as dtdate
        date_from = options["date_from"] or str(dtdate.today())
        date_to = options["date_to"] or date_from

        headers = {"x-api-key": api_key}
        skip_messages = options["skip_messages"]
        max_pages = options["max_pages"]

        t0 = time.time()
        total_chats = 0
        total_msgs = 0
        total_buffered = 0

        # Phase 1: fetch all chats in date range
        self.stdout.write(f"Fetching chats updated {date_from} to {date_to}...")
        url = f"{SMCLICK_API}/attendances/chats"
        params = {"update_time_range": f"{date_from},{date_to}"}
        page = 0

        all_chats = []
        while url and page < max_pages:
            page += 1
            resp = requests.get(url, headers=headers, params=params, timeout=30)
            if resp.status_code != 200:
                self.stderr.write(f"  API error {resp.status_code}: {resp.text[:200]}")
                break
            data = resp.json()
            results = data.get("results", [])
            all_chats.extend(results)
            count = data.get("count", 0)
            self.stdout.write(f"  Page {page}: {len(results)} chats (total in API: {count})")

            url = data.get("next")
            params = {}  # next URL already has params
            if url:
                time.sleep(0.2)  # rate limit courtesy

        total_chats = len(all_chats)
        self.stdout.write(f"Fetched {total_chats} chats")

        # Phase 2: for each chat, build a webhook-compatible payload and buffer it
        self.stdout.write("Buffering chat events...")
        with connection.cursor() as cur:
            for i, chat in enumerate(all_chats):
                chat_id = chat.get("id")
                if not chat_id:
                    continue

                # Build chat payload in webhook format
                chat_payload = self._build_chat_payload(chat)
                buffered = self._insert_buffer(cur, chat_payload, chat_id)
                if buffered:
                    total_buffered += 1

                if (i + 1) % 500 == 0:
                    self.stdout.write(f"  Buffered {i + 1}/{total_chats} chats...")

        self.stdout.write(f"Buffered {total_buffered} chat events")

        # Phase 3: fetch messages for each chat
        if not skip_messages:
            self.stdout.write("Fetching messages...")
            with connection.cursor() as cur:
                for i, chat in enumerate(all_chats):
                    chat_id = chat.get("id")
                    protocol = chat.get("protocol")
                    if not chat_id or not protocol:
                        continue

                    msgs = self._fetch_messages(headers, protocol)
                    for msg in msgs:
                        msg_payload = self._build_message_payload(chat, msg)
                        if self._insert_buffer(cur, msg_payload, chat_id, msg.get("id")):
                            total_msgs += 1

                    if (i + 1) % 100 == 0:
                        self.stdout.write(
                            f"  {i + 1}/{total_chats} chats processed, "
                            f"{total_msgs} messages buffered, "
                            f"elapsed={time.time()-t0:.0f}s"
                        )
                    if msgs:
                        time.sleep(0.15)  # rate limit

        elapsed = time.time() - t0
        self.stdout.write(self.style.SUCCESS(
            f"Done: {total_chats} chats, {total_msgs} messages buffered "
            f"in {elapsed:.1f}s. Run sync_smclick to process."
        ))

    def _build_chat_payload(self, chat):
        """Convert API chat response to webhook-compatible payload."""
        # Map attendant from API format (list or single) to webhook format (array)
        attendant_raw = chat.get("attendant") or chat.get("attendants") or []
        if isinstance(attendant_raw, dict):
            attendant_raw = [attendant_raw]

        attendant_list = []
        for att in attendant_raw:
            if isinstance(att, dict):
                attendant_list.append({
                    "id": att.get("id", ""),
                    "name": att.get("name", ""),
                    "email": att.get("email", ""),
                    "principal": str(att.get("principal", False)).lower(),
                })

        contact = chat.get("contact") or {}
        department = chat.get("department") or {}
        crm_column = chat.get("crm_column") or {}

        return {
            "event": "backfill_chat",
            "event_time": chat.get("updated_at"),
            "infos": {
                "chat": {
                    "id": chat.get("id"),
                    "protocol": str(chat.get("protocol", "")),
                    "status": chat.get("status"),
                    "flow": chat.get("flow"),
                    "department": {
                        "id": department.get("id", ""),
                        "name": department.get("name", ""),
                    },
                    "contact": {
                        "id": contact.get("id", ""),
                        "name": contact.get("name", ""),
                        "telephone": contact.get("telephone", ""),
                        "tags": contact.get("tags", []),
                        "segmentation_fields": contact.get("segmentation_fields", []),
                    },
                    "attendant": attendant_list,
                    "crm_column": {
                        "name": crm_column.get("name", "") if isinstance(crm_column, dict) else "",
                    } if crm_column else None,
                    "finish_reason": {
                        "reason": (chat.get("finish_reason") or {}).get("reason", "")
                    } if chat.get("finish_reason") else None,
                    "created_at": chat.get("created_at"),
                    "updated_at": chat.get("updated_at"),
                    "annotations": chat.get("annotations", []),
                },
            },
        }

    def _build_message_payload(self, chat, msg):
        """Convert API message response to webhook-compatible payload."""
        content = msg.get("content") or {}
        sent_by = msg.get("sent_by") or {}

        return {
            "event": "backfill_message",
            "event_time": msg.get("sent_at"),
            "infos": {
                "chat": {
                    "id": chat.get("id"),
                    "protocol": str(chat.get("protocol", "")),
                    "status": chat.get("status"),
                },
                "message": {
                    "id": msg.get("id"),
                    "type": msg.get("type"),
                    "stage": msg.get("stage"),
                    "from_me": str(msg.get("from_me", False)).lower(),
                    "status": str(msg.get("status", True)).lower(),
                    "content": {
                        "text": content.get("text", ""),
                        "original_text": content.get("original_text", ""),
                    },
                    "sent_at": msg.get("sent_at"),
                    "sent_by": {
                        "name": sent_by.get("name", "") if isinstance(sent_by, dict) else "",
                        "email": sent_by.get("email", "") if isinstance(sent_by, dict) else "",
                    } if sent_by else {},
                    "fail_reason": msg.get("fail_reason") or "",
                    "quoted": msg.get("quoted"),
                },
            },
        }

    def _fetch_messages(self, headers, protocol):
        """Fetch all messages for a chat by protocol, paginating."""
        all_msgs = []
        url = f"{SMCLICK_API}/attendances/chats/message"
        params = {"protocol": protocol}

        while url:
            try:
                resp = requests.get(url, headers=headers, params=params, timeout=30)
                if resp.status_code != 200:
                    break
                data = resp.json()
                all_msgs.extend(data.get("results", []))
                url = data.get("next")
                params = {}
                if url:
                    time.sleep(0.1)
            except Exception:
                break

        return all_msgs

    def _insert_buffer(self, cursor, payload, chat_id, message_id=None):
        """Insert a payload into smclick_ingest_buffer with dedup."""
        payload_text = json.dumps(payload, ensure_ascii=False, default=str)
        payload_hash = hashlib.sha256(payload_text.encode("utf-8")).hexdigest()
        event_name = payload.get("event")
        event_time = payload.get("event_time")

        cursor.execute(
            """
            INSERT INTO smclick_ingest_buffer (
                payload_hash, payload, event_name, event_time,
                chat_id, message_id
            ) VALUES (
                %s, %s::jsonb, %s,
                %s::timestamptz, %s::uuid, %s
            )
            ON CONFLICT (payload_hash) DO NOTHING
            RETURNING id
            """,
            [payload_hash, payload_text, event_name,
             event_time, chat_id, message_id],
        )
        return cursor.fetchone() is not None
