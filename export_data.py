"""
Migration script: exports all data from the local SQLite database
and imports it into the PostgreSQL database on Railway.

Usage:
  DATABASE_URL="postgresql://..." python export_data.py
"""
import os
import sqlite3
import sys

# ─── SOURCE: local SQLite ─────────────────────────────────────────────────────
LOCAL_DB = os.path.join(os.path.dirname(__file__), 'database.db')

if not os.path.exists(LOCAL_DB):
    print("❌ database.db not found. Nothing to migrate.")
    sys.exit(1)

src = sqlite3.connect(LOCAL_DB)
src.row_factory = sqlite3.Row

# ─── DESTINATION: PostgreSQL (from DATABASE_URL env) ─────────────────────────
DATABASE_URL = os.environ.get('DATABASE_URL', '')
if not DATABASE_URL:
    print("❌ Set DATABASE_URL environment variable to your Railway PostgreSQL URL.")
    sys.exit(1)

if DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

import psycopg2
dst = psycopg2.connect(DATABASE_URL)
dst.autocommit = False
cur = dst.cursor()

# ─── MIGRATE ──────────────────────────────────────────────────────────────────

# Create tables on destination
cur.execute('''
    CREATE TABLE IF NOT EXISTS professionals (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        role TEXT NOT NULL,
        email TEXT,
        phone TEXT,
        notes TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
''')
cur.execute('''
    CREATE TABLE IF NOT EXISTS payments (
        id SERIAL PRIMARY KEY,
        professional_id INTEGER NOT NULL,
        project_id INTEGER,
        project_name TEXT,
        amount REAL NOT NULL,
        status TEXT DEFAULT 'pago',
        payment_date TEXT,
        notes TEXT
    )
''')

# Migrate professionals (keeping original IDs via sequence reset)
professionals = src.execute("SELECT * FROM professionals ORDER BY id").fetchall()
print(f"Migrating {len(professionals)} professionals...")

id_map = {}  # old_id -> new_id
for p in professionals:
    cur.execute(
        "INSERT INTO professionals (name, role, email, phone, notes, created_at) VALUES (%s,%s,%s,%s,%s,%s) RETURNING id",
        (p['name'], p['role'], p['email'], p['phone'], p['notes'], p['created_at'])
    )
    new_id = cur.fetchone()[0]
    id_map[p['id']] = new_id

# Migrate payments
payments = src.execute("SELECT * FROM payments ORDER BY id").fetchall()
print(f"Migrating {len(payments)} payments...")

for pay in payments:
    new_prof_id = id_map.get(pay['professional_id'], pay['professional_id'])
    cur.execute(
        "INSERT INTO payments (professional_id, project_id, project_name, amount, status, payment_date, notes) VALUES (%s,%s,%s,%s,%s,%s,%s)",
        (new_prof_id, pay['project_id'], pay['project_name'], pay['amount'],
         pay['status'], pay['payment_date'], pay['notes'])
    )

dst.commit()
src.close()
dst.close()

print(f"\n✅ Migration complete!")
print(f"   {len(professionals)} professionals")
print(f"   {len(payments)} payments")
