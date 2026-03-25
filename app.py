import os
import sys
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from werkzeug.utils import secure_filename

sys.path.insert(0, os.path.dirname(__file__))

from models import (
    init_db, get_all_roles, search_professionals,
    get_professional_avg_rate, suggest_team, get_role_stats,
    get_dashboard_stats, get_payments_for_professional, get_total_received,
    _engine, _insert, text
)
from importer import read_excel_preview, import_from_excel, get_all_sheets

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'producao-filmes-2024')
app.jinja_env.globals['enumerate'] = enumerate

UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'uploads')
ALLOWED_EXTENSIONS = {'xlsx', 'xls'}
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Initialize DB on startup (works with both WSGI and direct run)
init_db()

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


# ─── DASHBOARD ────────────────────────────────────────────────────────────────

@app.route('/')
def dashboard():
    stats = get_dashboard_stats()
    return render_template('index.html', **stats)


# ─── PROFESSIONALS ────────────────────────────────────────────────────────────

@app.route('/professionals')
def professionals():
    query = request.args.get('q', '')
    role = request.args.get('role', '')
    max_rate = request.args.get('max_rate', None)
    if max_rate:
        try:
            max_rate = float(max_rate)
        except:
            max_rate = None

    profs = search_professionals(query=query, role=role, max_rate=max_rate)
    roles = get_all_roles()

    return render_template('professionals.html',
        professionals=profs, roles=roles,
        query=query, selected_role=role, max_rate=max_rate)


@app.route('/professionals/new', methods=['GET', 'POST'])
def new_professional():
    if request.method == 'POST':
        name = request.form.get('name', '').strip()
        role = request.form.get('role', '').strip()
        email = request.form.get('email', '').strip()
        phone = request.form.get('phone', '').strip()
        notes = request.form.get('notes', '').strip()

        if not name or not role:
            flash('Nome e cargo são obrigatórios.', 'danger')
            return redirect(url_for('new_professional'))

        with _engine.begin() as conn:
            _insert(conn,
                "INSERT INTO professionals (name, role, email, phone, notes) VALUES (:name, :role, :email, :phone, :notes)",
                {"name": name, "role": role, "email": email, "phone": phone, "notes": notes}
            )

        flash(f'Profissional "{name}" adicionado com sucesso!', 'success')
        return redirect(url_for('professionals'))

    roles = get_all_roles()
    return render_template('professional_form.html', professional=None, roles=roles, action='new')


@app.route('/professionals/<int:prof_id>')
def professional_detail(prof_id):
    with _engine.connect() as conn:
        prof = conn.execute(
            text("SELECT * FROM professionals WHERE id = :id"), {"id": prof_id}
        ).fetchone()

    if not prof:
        flash('Profissional não encontrado.', 'danger')
        return redirect(url_for('professionals'))

    prof = dict(prof._mapping)
    payments = get_payments_for_professional(prof_id)
    avg_rate, total_projects = get_professional_avg_rate(prof_id)
    total_received = get_total_received(prof_id)

    return render_template('professional_detail.html',
        prof=prof, payments=payments,
        avg_rate=avg_rate, total_projects=total_projects,
        total_received=total_received)


@app.route('/professionals/<int:prof_id>/edit', methods=['GET', 'POST'])
def edit_professional(prof_id):
    with _engine.connect() as conn:
        prof = conn.execute(
            text("SELECT * FROM professionals WHERE id = :id"), {"id": prof_id}
        ).fetchone()

    if not prof:
        flash('Profissional não encontrado.', 'danger')
        return redirect(url_for('professionals'))

    prof = dict(prof._mapping)

    if request.method == 'POST':
        name = request.form.get('name', '').strip()
        role = request.form.get('role', '').strip()
        email = request.form.get('email', '').strip()
        phone = request.form.get('phone', '').strip()
        notes = request.form.get('notes', '').strip()

        with _engine.begin() as conn:
            conn.execute(text(
                "UPDATE professionals SET name=:name, role=:role, email=:email, phone=:phone, notes=:notes WHERE id=:id"
            ), {"name": name, "role": role, "email": email, "phone": phone, "notes": notes, "id": prof_id})

        flash('Profissional atualizado com sucesso!', 'success')
        return redirect(url_for('professional_detail', prof_id=prof_id))

    roles = get_all_roles()
    return render_template('professional_form.html', professional=prof, roles=roles, action='edit')


@app.route('/professionals/<int:prof_id>/delete', methods=['POST'])
def delete_professional(prof_id):
    with _engine.begin() as conn:
        row = conn.execute(text("SELECT name FROM professionals WHERE id = :id"), {"id": prof_id}).fetchone()
        if row:
            conn.execute(text("DELETE FROM payments WHERE professional_id = :id"), {"id": prof_id})
            conn.execute(text("DELETE FROM professionals WHERE id = :id"), {"id": prof_id})
            flash(f'Profissional "{row[0]}" removido.', 'success')
    return redirect(url_for('professionals'))


@app.route('/professionals/<int:prof_id>/payments/add', methods=['POST'])
def add_payment(prof_id):
    project_name = request.form.get('project_name', '').strip()
    amount_raw = request.form.get('amount', '0').replace(',', '.').strip()
    status = request.form.get('status', 'pago')
    payment_date = request.form.get('payment_date', '').strip()
    notes = request.form.get('notes', '').strip()

    try:
        amount = float(amount_raw)
    except:
        flash('Valor inválido.', 'danger')
        return redirect(url_for('professional_detail', prof_id=prof_id))

    with _engine.begin() as conn:
        _insert(conn,
            "INSERT INTO payments (professional_id, project_name, amount, status, payment_date, notes) VALUES (:pid, :pname, :amount, :status, :date, :notes)",
            {"pid": prof_id, "pname": project_name, "amount": amount, "status": status, "date": payment_date, "notes": notes}
        )

    flash('Pagamento registrado com sucesso!', 'success')
    return redirect(url_for('professional_detail', prof_id=prof_id))


@app.route('/payments/<int:payment_id>/delete', methods=['POST'])
def delete_payment(payment_id):
    with _engine.begin() as conn:
        row = conn.execute(text("SELECT professional_id FROM payments WHERE id = :id"), {"id": payment_id}).fetchone()
        if row:
            prof_id = row[0]
            conn.execute(text("DELETE FROM payments WHERE id = :id"), {"id": payment_id})
            flash('Pagamento removido.', 'success')
            return redirect(url_for('professional_detail', prof_id=prof_id))
    return redirect(url_for('professionals'))


# ─── BUDGET PLANNER ───────────────────────────────────────────────────────────

@app.route('/planner')
def planner():
    roles = get_all_roles()
    return render_template('planner.html', roles=roles)


@app.route('/planner/suggest', methods=['POST'])
def planner_suggest():
    data = request.get_json()
    budget = float(data.get('budget', 0))
    roles_needed = {k: int(v) for k, v in data.get('roles', {}).items() if int(v) > 0}

    if not budget or not roles_needed:
        return jsonify({'error': 'Informe o budget e pelo menos uma função.'}), 400

    role_stats = {}
    for role in roles_needed:
        stats = get_role_stats(role)
        if stats:
            role_stats[role] = {'min': stats['min_rate'], 'avg': stats['avg_rate'], 'max': stats['max_rate']}

    teams = suggest_team(roles_needed, budget)
    return jsonify({'teams': teams, 'role_stats': role_stats, 'budget': budget})


@app.route('/planner/roles/stats')
def role_stats_api():
    role = request.args.get('role', '')
    if not role:
        return jsonify({})
    stats = get_role_stats(role)
    if stats:
        return jsonify({'min': stats['min_rate'], 'avg': stats['avg_rate'], 'max': stats['max_rate']})
    return jsonify({})


# ─── IMPORT ───────────────────────────────────────────────────────────────────

@app.route('/import', methods=['GET', 'POST'])
def import_excel():
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('Nenhum arquivo enviado.', 'danger')
            return redirect(url_for('import_excel'))

        file = request.files['file']
        if file.filename == '' or not allowed_file(file.filename):
            flash('Arquivo inválido. Use .xlsx ou .xls', 'danger')
            return redirect(url_for('import_excel'))

        filename = secure_filename(file.filename)
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        file.save(filepath)

        preview = read_excel_preview(filepath)
        sheets = get_all_sheets(filepath)

        return render_template('import_map.html',
            filename=filename, filepath=filepath,
            columns=preview['columns'], preview_rows=preview['preview'],
            sheets=sheets, error=preview.get('error'))

    return render_template('import.html')


@app.route('/import/process', methods=['POST'])
def import_process():
    filename = request.form.get('filename')
    sheet_name = request.form.get('sheet_name', 0)
    try:
        sheet_name = int(sheet_name)
    except:
        pass

    filepath = os.path.join(UPLOAD_FOLDER, filename)
    column_map = {}
    for field in ['name', 'role', 'email', 'phone', 'amount', 'project_name', 'status', 'payment_date', 'notes']:
        val = request.form.get(f'col_{field}', '').strip()
        if val and val != '__none__':
            column_map[field] = val

    result = import_from_excel(filepath, column_map, sheet_name=sheet_name)

    try:
        os.remove(filepath)
    except:
        pass

    flash(f'Importação concluída: {result["imported"]} registros importados, {result["skipped"]} ignorados.',
          'success' if result['imported'] > 0 else 'warning')

    for err in result.get('errors', [])[:5]:
        flash(f'Aviso: {err}', 'warning')

    return redirect(url_for('professionals'))


# ─── API ──────────────────────────────────────────────────────────────────────

@app.route('/api/roles')
def api_roles():
    return jsonify(get_all_roles())


# ─── MAIN ─────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_ENV', 'production') == 'development'
    print(f"\n🎬 Production Manager rodando em http://localhost:{port}\n")
    app.run(host='0.0.0.0', port=port, debug=debug)
