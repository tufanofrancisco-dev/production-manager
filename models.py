import os
from sqlalchemy import create_engine, text, event

# ─── DATABASE SETUP ────────────────────────────────────────────────────────────
DATABASE_URL = os.environ.get('DATABASE_URL', '')

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
        # Professionals
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

        # Projects
        conn.execute(text(f'''
            CREATE TABLE IF NOT EXISTS projects (
                id {id_col},
                name TEXT NOT NULL,
                client TEXT,
                director TEXT,
                start_date TEXT,
                end_date TEXT,
                budget REAL,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        '''))

        # Payments
        conn.execute(text(f'''
            CREATE TABLE IF NOT EXISTS payments (
                id {id_col},
                professional_id INTEGER NOT NULL,
                project_id INTEGER,
                project_name TEXT,
                amount REAL NOT NULL,
                status TEXT DEFAULT 'pago',
                payment_date TEXT,
                notes TEXT,
                rating INTEGER
            )
        '''))

    # ── Step 2: Add missing columns (each in its own transaction) ─────────────
    # Separate transactions: a failure on one column never aborts the others.
    for pg_sql, lite_sql in [
        (
            "ALTER TABLE payments ADD COLUMN IF NOT EXISTS rating INTEGER",
            "ALTER TABLE payments ADD COLUMN rating INTEGER",
        ),
        (
            "ALTER TABLE payments ADD COLUMN IF NOT EXISTS project_id INTEGER",
            "ALTER TABLE payments ADD COLUMN project_id INTEGER",
        ),
    ]:
        try:
            with _engine.begin() as conn:
                conn.execute(text(pg_sql if IS_POSTGRES else lite_sql))
        except Exception:
            pass  # Column already exists – safe to ignore

    # ── Step 3: Populate projects from existing payment project_names ──────────
    # Own transaction so failures here never block the app from starting.
    try:
        with _engine.begin() as conn:
            distinct = conn.execute(text('''
                SELECT DISTINCT project_name FROM payments
                WHERE project_name IS NOT NULL AND project_name != ''
                AND project_id IS NULL
            ''')).fetchall()

            for row in distinct:
                pname = row[0].strip() if row[0] else None
                if not pname:
                    continue
                existing = conn.execute(
                    text("SELECT id FROM projects WHERE name = :name"),
                    {"name": pname}
                ).fetchone()
                if existing:
                    proj_id = existing[0]
                else:
                    proj_id = _insert(conn,
                        "INSERT INTO projects (name) VALUES (:name)",
                        {"name": pname}
                    )
                conn.execute(
                    text("UPDATE payments SET project_id = :pid WHERE project_name = :pname AND project_id IS NULL"),
                    {"pid": proj_id, "pname": pname}
                )
    except Exception:
        pass  # Migration already done or no data yet


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


def get_professional_avg_rating(professional_id):
    with _engine.connect() as conn:
        result = conn.execute(
            text("SELECT AVG(rating) as avg_rating, COUNT(rating) as cnt FROM payments WHERE professional_id = :id AND rating IS NOT NULL"),
            {"id": professional_id}
        )
        row = result.fetchone()
        return (round(row[0], 1) if row[0] else None, row[1] or 0)


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
    sql = f'''
        SELECT pr.id, pr.name, pr.role, pr.email, pr.phone, pr.notes, pr.created_at,
               AVG(p.amount) as avg_rate,
               COUNT(p.id) as total_projects,
               AVG(p.rating) as avg_rating
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


def update_payment_rating(payment_id, rating):
    with _engine.begin() as conn:
        conn.execute(
            text("UPDATE payments SET rating = :rating WHERE id = :id"),
            {"rating": rating, "id": payment_id}
        )


# ─── PROJECTS ─────────────────────────────────────────────────────────────────

def get_all_projects():
    with _engine.connect() as conn:
        group_by = "pj.id, pj.name, pj.client, pj.director, pj.start_date, pj.end_date, pj.budget, pj.created_at" if IS_POSTGRES else "pj.id"
        result = conn.execute(text(f'''
            SELECT pj.id, pj.name, pj.client, pj.director,
                   pj.start_date, pj.end_date, pj.budget, pj.created_at,
                   COUNT(DISTINCT py.professional_id) as professional_count,
                   COALESCE(SUM(py.amount), 0) as total_spent,
                   COUNT(py.id) as payment_count
            FROM projects pj
            LEFT JOIN payments py ON py.project_id = pj.id
            GROUP BY {group_by}
            ORDER BY pj.created_at DESC
        '''))
        return _rows(result)


def get_project(project_id):
    with _engine.connect() as conn:
        return _row(conn.execute(
            text("SELECT * FROM projects WHERE id = :id"),
            {"id": project_id}
        ))


def get_project_detail(project_id):
    """Returns project + all professionals with their payment/rating for this project."""
    with _engine.connect() as conn:
        project = _row(conn.execute(
            text("SELECT * FROM projects WHERE id = :id"),
            {"id": project_id}
        ))
        if not project:
            return None, []

        group_by = "pr.id, pr.name, pr.role, pr.email, py.id, py.amount, py.status, py.payment_date, py.rating, py.notes" if IS_POSTGRES else "py.id"
        crew = _rows(conn.execute(text(f'''
            SELECT pr.id as prof_id, pr.name, pr.role, pr.email,
                   py.id as payment_id, py.amount, py.status,
                   py.payment_date, py.rating, py.notes as pay_notes
            FROM payments py
            JOIN professionals pr ON py.professional_id = pr.id
            WHERE py.project_id = :project_id
            ORDER BY pr.role, pr.name
        '''), {"project_id": project_id}))

        # Summary stats
        stats = _row(conn.execute(text('''
            SELECT COUNT(DISTINCT professional_id) as prof_count,
                   COALESCE(SUM(amount), 0) as total_spent
            FROM payments WHERE project_id = :project_id
        '''), {"project_id": project_id}))

        project['prof_count'] = stats['prof_count'] if stats else 0
        project['total_spent'] = stats['total_spent'] if stats else 0

        return project, crew


def create_project(name, client='', director='', start_date='', end_date='', budget=None, notes=''):
    with _engine.begin() as conn:
        return _insert(conn,
            "INSERT INTO projects (name, client, director, start_date, end_date, budget, notes) VALUES (:name, :client, :director, :start_date, :end_date, :budget, :notes)",
            {"name": name, "client": client, "director": director,
             "start_date": start_date or None, "end_date": end_date or None,
             "budget": budget, "notes": notes}
        )


def update_project(project_id, name, client='', director='', start_date='', end_date='', budget=None, notes=''):
    with _engine.begin() as conn:
        conn.execute(text(
            "UPDATE projects SET name=:name, client=:client, director=:director, start_date=:start_date, end_date=:end_date, budget=:budget, notes=:notes WHERE id=:id"
        ), {"name": name, "client": client, "director": director,
            "start_date": start_date or None, "end_date": end_date or None,
            "budget": budget, "id": project_id, "notes": notes})


def delete_project(project_id):
    with _engine.begin() as conn:
        # Unlink payments (keep them, just remove project association)
        conn.execute(text("UPDATE payments SET project_id = NULL WHERE project_id = :id"), {"id": project_id})
        conn.execute(text("DELETE FROM projects WHERE id = :id"), {"id": project_id})


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
        total_projects = conn.execute(text("SELECT COUNT(*) FROM projects")).fetchone()[0]
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
        'total_projects': total_projects,
        'total_paid': total_paid or 0,
        'total_roles': total_roles,
        'top_roles': top_roles,
        'recent_professionals': recent,
    }
