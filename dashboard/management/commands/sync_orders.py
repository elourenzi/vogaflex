"""
Django management command: sync_orders

Fetches orders from the Vogaflex e-commerce API (WTEK) and upserts them
into the vogaflex_order table. Matches orders to WhatsApp chats via
normalized phone number.

Usage:
  python manage.py sync_orders                        # last 30 days
  python manage.py sync_orders --days 90              # last 90 days
  python manage.py sync_orders --de 2026-01-01 --ate 2026-03-19

Schedule with OS cron every 30 minutes:
  */30 * * * * cd /app && python manage.py sync_orders >> /var/log/sync_orders.log 2>&1
"""
import os
import re
import time
import requests
from datetime import datetime, timedelta

from django.core.management.base import BaseCommand
from django.db import connection


API_URL = "https://vogaflex.com.br/api"
API_USER = os.environ.get("VOGAFLEX_API_USER", "ef.lourenzi@gmail.com")
API_PASS = os.environ.get("VOGAFLEX_API_PASS", "1z#vLdRd")


def normalize_phone(raw):
    """Strip non-digits, ensure 55 prefix, return digits-only string."""
    if not raw:
        return None
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 10 or len(digits) == 11:
        digits = "55" + digits
    return digits if len(digits) >= 12 else None


class Command(BaseCommand):
    help = "Sync orders from Vogaflex e-commerce API into vogaflex_order"

    def add_arguments(self, parser):
        parser.add_argument("--days", type=int, default=30,
                            help="How many days back to fetch (default: 30)")
        parser.add_argument("--de", type=str, default=None,
                            help="Start date YYYY-MM-DD (overrides --days)")
        parser.add_argument("--ate", type=str, default=None,
                            help="End date YYYY-MM-DD (default: today)")
        parser.add_argument("--page-size", type=int, default=100,
                            help="Records per API page (default: 100)")

    def handle(self, *args, **options):
        t0 = time.time()
        today = datetime.now().strftime("%Y-%m-%d")
        date_from = options["de"] or (
            datetime.now() - timedelta(days=options["days"])
        ).strftime("%Y-%m-%d")
        date_to = options["ate"] or today
        page_size = options["page_size"]

        self.stdout.write(f"Fetching orders from {date_from} to {date_to}...")

        page = 1
        total_fetched = 0
        all_orders = []

        while True:
            resp = requests.get(
                API_URL,
                params={
                    "rota": "pedidos",
                    "page": page,
                    "limit": page_size,
                    "de": date_from,
                    "ate": date_to,
                },
                auth=(API_USER, API_PASS),
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()

            pedidos = data.get("pedidos", [])
            if not pedidos:
                break

            all_orders.extend(pedidos)
            total_fetched += len(pedidos)
            total_pages = data.get("total_paginas", 1)

            self.stdout.write(
                f"  Page {page}/{total_pages}: {len(pedidos)} orders "
                f"(total: {total_fetched}/{data.get('total_registros', '?')})"
            )

            if page >= total_pages:
                break
            page += 1

        if not all_orders:
            self.stdout.write(self.style.WARNING("No orders found."))
            return

        # Upsert into database
        upsert_sql = """
            INSERT INTO vogaflex_order (
                order_id, created_at, updated_at, status,
                valor_itens, valor_entrega, valor_desconto,
                vendedor_id, vendedor_nome,
                cliente_id, cliente_nome, cliente_cpf, cliente_email,
                cliente_telefone, telefone_norm, synced_at
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, %s, %s,
                %s, %s, NOW()
            )
            ON CONFLICT (order_id) DO UPDATE SET
                updated_at = EXCLUDED.updated_at,
                status = EXCLUDED.status,
                valor_itens = EXCLUDED.valor_itens,
                valor_entrega = EXCLUDED.valor_entrega,
                valor_desconto = EXCLUDED.valor_desconto,
                vendedor_id = EXCLUDED.vendedor_id,
                vendedor_nome = EXCLUDED.vendedor_nome,
                cliente_nome = EXCLUDED.cliente_nome,
                cliente_cpf = EXCLUDED.cliente_cpf,
                cliente_email = EXCLUDED.cliente_email,
                cliente_telefone = EXCLUDED.cliente_telefone,
                telefone_norm = EXCLUDED.telefone_norm,
                synced_at = NOW()
        """

        with connection.cursor() as cur:
            for p in all_orders:
                cliente = p.get("cliente") or {}
                telefone_raw = cliente.get("TELEFONE", "")
                cur.execute(upsert_sql, [
                    p["ID"],
                    p.get("CREATED_AT"),
                    p.get("UPDATED_AT"),
                    p.get("STATUS"),
                    p.get("VALOR_ITENS"),
                    p.get("VALOR_ENTREGA"),
                    p.get("VALOR_CUPOM_DESCONTO"),
                    p.get("VENDEDOR"),
                    p.get("NOME_VENDEDOR"),
                    cliente.get("ID"),
                    cliente.get("NOME"),
                    cliente.get("CPF"),
                    cliente.get("EMAIL"),
                    telefone_raw,
                    normalize_phone(telefone_raw),
                ])

        elapsed = time.time() - t0
        self.stdout.write(
            self.style.SUCCESS(
                f"Done: {total_fetched} orders synced in {elapsed:.1f}s"
            )
        )
