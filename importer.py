"""
Excel importer — works with both SQLite (local) and PostgreSQL (production).
"""
import os
import pandas as pd
from models import _engine, _insert, text

UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'uploads')


def read_excel_preview(filepath, max_rows=5):
    try:
        df = pd.read_excel(filepath, nrows=max_rows, dtype=str).fillna('')
        return {'columns': list(df.columns), 'preview': df.values.tolist(), 'error': None}
    except Exception as e:
        return {'columns': [], 'preview': [], 'error': str(e)}


def get_all_sheets(filepath):
    try:
        return pd.ExcelFile(filepath).sheet_names
    except:
        return ['Sheet1']


def import_from_excel(filepath, column_map, sheet_name=0):
    try:
        df = pd.read_excel(filepath, sheet_name=sheet_name, dtype=str).fillna('')
    except Exception as e:
        return {'imported': 0, 'skipped': 0, 'errors': [str(e)]}

    imported = skipped = 0
    errors = []

    with _engine.begin() as conn:
        for idx, row in df.iterrows():
            try:
                name_col = column_map.get('name')
                role_col = column_map.get('role')
                if not name_col or not role_col:
                    skipped += 1
                    continue

                name = str(row.get(name_col, '')).strip()
                role = str(row.get(role_col, '')).strip()
                if not name or not role:
                    skipped += 1
                    continue

                email = str(row.get(column_map.get('email', ''), '')).strip() if column_map.get('email') else ''
                phone = str(row.get(column_map.get('phone', ''), '')).strip() if column_map.get('phone') else ''
                notes = str(row.get(column_map.get('notes', ''), '')).strip() if column_map.get('notes') else ''

                # Check for existing professional
                existing = conn.execute(
                    text("SELECT id FROM professionals WHERE name = :name AND role = :role"),
                    {"name": name, "role": role}
                ).fetchone()

                if existing:
                    prof_id = existing[0]
                else:
                    prof_id = _insert(conn,
                        "INSERT INTO professionals (name, role, email, phone, notes) VALUES (:name, :role, :email, :phone, :notes)",
                        {"name": name, "role": role, "email": email, "phone": phone, "notes": notes}
                    )

                # Payment
                amount_col = column_map.get('amount')
                if amount_col and row.get(amount_col, ''):
                    amount_raw = str(row.get(amount_col, '')).replace('R$', '').replace('.', '').replace(',', '.').strip()
                    try:
                        amount = float(amount_raw)
                    except:
                        amount = None

                    if amount is not None:
                        project_name = str(row.get(column_map['project_name'], '')).strip() if column_map.get('project_name') else ''
                        status_raw = str(row.get(column_map.get('status', ''), '')).strip().lower() if column_map.get('status') else ''
                        status = 'pago' if status_raw in ['pago','paid','sim','yes','s','y','true','1',''] else 'pendente'
                        payment_date = str(row.get(column_map.get('payment_date', ''), '')).strip() if column_map.get('payment_date') else ''

                        _insert(conn,
                            "INSERT INTO payments (professional_id, project_name, amount, status, payment_date) VALUES (:pid, :pname, :amount, :status, :date)",
                            {"pid": prof_id, "pname": project_name, "amount": amount, "status": status, "date": payment_date}
                        )

                imported += 1
            except Exception as e:
                errors.append(f"Linha {idx + 2}: {str(e)}")
                skipped += 1

    return {'imported': imported, 'skipped': skipped, 'errors': errors}
