import os
from sqlalchemy import create_engine, text, event

# ─── DATABASE SETUP ────────────────────────────────────────────────────────────
# Railway sets DATABASE_URL automatically when a PostgreSQL addon is added.
# Locally, we fall back to SQLite.

DATABASE_URL = os.environ.get('DATABASE_URL', '')

# Fix Railway's legacy postgres:// prefix
if DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

if not DATABASE_URL:
    DB_PATH = os.path.join(os.path.dirname(__file__), 'database.db')
    DATABASE_URL = f'sqlite:///{DB_PATH}'

IS_POSTGRES = DATABASE_URL.startswith('postgresql')

if IS_POSTGRES:
    _engine = create_engine(DATABASE_URL)
else:
    _engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})

    @event.listens_for(_engine, "connect")
    def _enable_fk(dbapi_conn, _):
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA foreign_keys=ON")
        cur.close()


# ─── HELPERS ──────────────────────────────────────────────────────────────────

def _rows(result):
    return [dict(row._mapping) for row in result.fetchall()]

def _row(result):
    row = result.fetchone()
    return dict(row._mapping) if row else None

def _insert(conn, sql, params):
    """Execute INSERT and return the new row's ID."""
    if IS_POSTGRES:
        result = conn.execute(text(sql + " RETURNING id"), params)
        return result.scalar()
    else:
        result = conn.execute(text(sql), params)
        return result.lastrowid

_LIKE = "ILIKE" if IS_POSTGRES else "LIKE"
_GROUP_PROF = (
    "pr.id, pr.name, pr.role, pr.email, pr.phone, pr.notes, pr.created_at"
    if IS_POSTGRES else "pr.id"
)


# ─── INIT DB ──────────────────────────────────────────────────────────────────

def init_db():
    id_col = "SERIAL PRIMARY KEY" if IS_POSTGRES else "INTEGER PRIMARY KEY AUTOINCREMENT"

    with _engine.begin() as conn:
        conn.execute(text(f'''
            CREATE TABLE IF NOT EXISTS professionals (
                id {id_col},
                name TEXT NOT NULL,
                role TEXT NOT NULL,
                email TEXT,
                phone TEXT,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        '''))

        conn.execute(text(f'''
            CREATE TABLE IF NOT EXISTS payments (
                id {id_col},
                professional_id INTEGER NOT NULL,
                project_id INTEGER,
                project_name TEXT,
                amount REAL NOT NULL,
                status TEXT DEFAULT 'pago',
                payment_date TEXT,
                notes TEXT
            )
        '''))


# ─── ROLES ────────────────────────────────────────────────────────────────────

def get_all_roles():
    with _engine.connect() as conn:
        result = conn.execute(text("SELECT DISTINCT role FROM professionals ORDER BY role"))
        return [row[0] for row in result]


# ─── PROFESSIONALS ────────────────────────────────────────────────────────────

def get_professional_avg_rate(professional_id):
    with _engine.connect() as conn:
        result = conn.execute(
            text("SELECT AVG(amount) as avg, COUNT(*) as cnt FROM payments WHERE professional_id = :id"),
            {"id": professional_id}
        )
        row = result.fetchone()
        return (row[0] or 0, row[1] or 0)


def get_role_stats(role):
    with _engine.connect() as conn:
        result = conn.execute(text('''
            SELECT MIN(p.amount) as min_rate,
                   AVG(p.amount) as avg_rate,
                   MAX(p.amount) as max_rate
            FROM payments p
            JOIN professionals pr ON p.professional_id = pr.id
            WHERE pr.role = :role
        '''), {"role": role})
        return _row(result)


def search_professionals(query=None, role=None, max_rate=None):
    # Always select all professional columns; GROUP BY varies by dialect
    sql = f'''
        SELECT pr.id, pr.name, pr.role, pr.email, pr.phone, pr.notes, pr.created_at,
               AVG(p.amount) as avg_rate,
               COUNT(p.id) as total_projects
        FROM professionals pr
        LEFT JOIN payments p ON p.professional_id = pr.id
        WHERE 1=1
    '''
    params = {}

    if query:
        sql += f" AND (pr.name {_LIKE} :query OR pr.role {_LIKE} :query OR pr.notes {_LIKE} :query)"
        params['query'] = f'%{query}%'

    if role:
        sql += " AND pr.role = :role"
        params['role'] = role

    sql += f" GROUP BY {_GROUP_PROF}"

    if max_rate:
        sql += " HAVING AVG(p.amount) <= :max_rate OR AVG(p.amount) IS NULL"
        params['max_rate'] = max_rate

    sql += " ORDER BY pr.name"

    with _engine.connect() as conn:
        return _rows(conn.execute(text(sql), params))


# ─── PAYMENTS ─────────────────────────────────────────────────────────────────

def get_payments_for_professional(professional_id):
    with _engine.connect() as conn:
        result = conn.execute(
            text("SELECT * FROM payments WHERE professional_id = :id ORDER BY payment_date DESC, id DESC"),
            {"id": professional_id}
        )
        return _rows(result)


def get_total_received(professional_id):
    with _engine.connect() as conn:
        result = conn.execute(
            text("SELECT COALESCE(SUM(amount), 0) FROM payments WHERE professional_id = :id AND status = 'pago'"),
            {"id": professional_id}
        )
        return result.fetchone()[0] or 0


# ─── BUDGET PLANNER ───────────────────────────────────────────────────────────

def suggest_team(roles_needed: dict, budget: float):
    team_slots = []

    with _engine.connect() as conn:
        for role, qty in roles_needed.items():
            group_by = "pr.id, pr.name, pr.role, pr.email, pr.phone" if IS_POSTGRES else "pr.id"
            result = conn.execute(text(f'''
                SELECT pr.id, pr.name, pr.role, pr.email, pr.phone,
                       AVG(p.amount) as avg_rate,
                       COUNT(DISTINCT p.project_name) as total_projects
                FROM professionals pr
                LEFT JOIN payments p ON p.professional_id = pr.id
                WHERE pr.role = :role
                GROUP BY {group_by}
                ORDER BY AVG(p.amount) ASC
            '''), {"role": role})
            candidates = _rows(result)

            for _ in range(qty):
                team_slots.append({'role': role, 'candidates': candidates})

    teams = []
    for option in ['economy', 'balanced', 'premium']:
        team, total, missing = [], 0.0, []
        for slot in team_slots:
            rated = sorted([c for c in slot['candidates'] if c.get('avg_rate')],
                           key=lambda x: x['avg_rate'])
            no_rate = [c for c in slot['candidates'] if not c.get('avg_rate')]

            if option == 'economy' and rated:
                pick = rated[0]
            elif option == 'premium' and rated:
                pick = rated[-1]
            elif option == 'balanced' and rated:
                pick = rated[len(rated) // 2]
            elif no_rate:
                pick = {**no_rate[0], 'avg_rate': 0}
            else:
                missing.append(slot['role'])
                continue

            team.append(pick)
            total += pick.get('avg_rate') or 0

        teams.append({
            'option': option,
            'team': team,
            'total': total,
            'fits_budget': total <= budget,
            'missing_roles': missing
        })

    return teams


# ─── DASHBOARD STATS ──────────────────────────────────────────────────────────

def get_dashboard_stats():
    with _engine.connect() as conn:
        total_professionals = conn.execute(text("SELECT COUNT(*) FROM professionals")).fetchone()[0]
        total_payments = conn.execute(text("SELECT COUNT(*) FROM payments")).fetchone()[0]
        total_paid = conn.execute(text("SELECT COALESCE(SUM(amount), 0) FROM payments WHERE status='pago'")).fetchone()[0]
        total_roles = conn.execute(text("SELECT COUNT(DISTINCT role) FROM professionals")).fetchone()[0]

        top_roles = _rows(conn.execute(text(f'''
            SELECT pr.role, COUNT(*) as count, AVG(p.amount) as avg_rate
            FROM professionals pr
            LEFT JOIN payments p ON p.professional_id = pr.id
            GROUP BY pr.role
            ORDER BY count DESC
            LIMIT 8
        ''')))

        recent_sql = f'''
            SELECT pr.id, pr.name, pr.role, pr.email, pr.phone, pr.notes, pr.created_at,
                   AVG(p.amount) as avg_rate,
                   COUNT(p.id) as total_projects
            FROM professionals pr
            LEFT JOIN payments p ON p.professional_id = pr.id
            GROUP BY {_GROUP_PROF}
            ORDER BY pr.created_at DESC
            LIMIT 6
        '''
        recent = _rows(conn.execute(text(recent_sql)))

    return {
        'total_professionals': total_professionals,
        'total_payments': total_payments,
        'total_paid': total_paid or 0,
        'total_roles': total_roles,
        'top_roles': top_roles,
        'recent_professionals': recent,
    }
