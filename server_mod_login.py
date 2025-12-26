import duckdb
import threading
import time
import os
import csv
import io
from datetime import datetime
from functools import lru_cache
from flask import Flask, request, jsonify, make_response, render_template_string,session
import secrets
import uuid



# =====================================================
# CONFIG
# =====================================================
PORT = 5000
DB_DIR = "database_files"
os.makedirs(DB_DIR, exist_ok=True)

date_str = datetime.now().strftime("%d%b%Y").upper()
DB_FILE = os.path.join(DB_DIR, f"jmeter_metrics_{date_str}.duckdb")


def generate_secret_key():
    return secrets.token_hex(32)

# =====================================================
# APP & DB
# =====================================================
app = Flask(__name__)
app.secret_key = generate_secret_key() #for active session
DB_LOCK = threading.Lock()
CONN = duckdb.connect(DB_FILE)

# =====================================================
# INIT DB
# =====================================================
CURRENT_DATE = None
CONN = None
ACTIVE_SESSIONS = {}
ACTIVE_TABS = {}
TAB_TIMEOUT = 20  # seconds

def get_db():
    global CURRENT_DATE, CONN

    today = datetime.now().strftime("%d%b%Y").upper()

    if CURRENT_DATE != today:
        if CONN:
            CONN.close()

        db_file = os.path.join(DB_DIR, f"jmeter_metrics_{today}.duckdb")
        CONN = duckdb.connect(db_file)
        CURRENT_DATE = today

        # Ensure schema exists
        conn = get_db()
        CONN.execute("""
        CREATE TABLE IF NOT EXISTS jmeter_samples (
            timestamp BIGINT,
            label VARCHAR,
            response_time DOUBLE,
            success INTEGER,
            thread_count INTEGER,
            status_code INTEGER,
            error_message VARCHAR,
            received_bytes DOUBLE,
            sent_bytes DOUBLE,
            test_id VARCHAR
        )
        """)

    return CONN

def init_db():
    with DB_LOCK:
        conn = get_db()
        CONN.execute("""
        CREATE TABLE IF NOT EXISTS jmeter_samples (
            timestamp BIGINT,
            label VARCHAR,
            response_time DOUBLE,
            success INTEGER,
            thread_count INTEGER,
            status_code INTEGER,
            error_message VARCHAR,
            received_bytes DOUBLE,
            sent_bytes DOUBLE,
            test_id VARCHAR
        )
        """)
        CONN.execute("CREATE INDEX IF NOT EXISTS idx_ts_test ON jmeter_samples(timestamp, test_id)")
        CONN.execute("CREATE INDEX IF NOT EXISTS idx_label_test ON jmeter_samples(label, test_id)")
        CONN.execute("CREATE INDEX IF NOT EXISTS idx_test ON jmeter_samples(test_id)")

init_db()

# =====================================================
# HELPERS
# =====================================================
def run_query(sql, params=()):
    with DB_LOCK:
        conn = get_db()
        return CONN.execute(sql, params).fetchall()

def clear_cache():
    cached_aggregate.cache_clear()
    cached_tps.cache_clear()
    cached_threads.cache_clear()
    cached_errorpct.cache_clear()
    cached_label_tps.cache_clear()
def choose_bucket(start, end):
    duration = end - start
    if duration <= 300:       # <= 5 min
        return 1
    elif duration <= 1800:    # <= 30 min
        return 5
    elif duration <= 7200:    # <= 2 hr
        return 10
    else:
        return 30

# =====================================================
# INGEST (JMeter)
# =====================================================
@app.before_request
def track_active_tabs():
    tab_id = request.args.get("tab_id")
    if not tab_id:
        return

    now = time.time()
    ACTIVE_TABS[tab_id] = now

    expired = [k for k, v in ACTIVE_TABS.items() if now - v > TAB_TIMEOUT]
    for k in expired:
        ACTIVE_TABS.pop(k, None)
@app.route("/api/active_sessions")
def active_sessions():
    now = time.time()
    count = sum(1 for v in ACTIVE_TABS.values() if now - v <= TAB_TIMEOUT)
    return jsonify({"active_sessions": count})


@app.route("/metrics", methods=["POST"])
def ingest():
    d = request.get_json(force=True)
    with DB_LOCK:
        conn = get_db()
        CONN.execute("""
        INSERT INTO jmeter_samples VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            d.get("timestamp"),
            d.get("label"),
            d.get("response_time"),
            d.get("success"),
            d.get("thread_count"),
            d.get("status_code"),
            d.get("error_message"),
            d.get("received_bytes", 0),
            d.get("sent_bytes", 0),
            d.get("test_id", "default")
        ))
    clear_cache()
    return jsonify({"status": "ok"})

# =====================================================
# AGGREGATE (FAST)
# =====================================================
@lru_cache(maxsize=128)
def cached_aggregate(test_id, start, end):
    return run_query("""
    SELECT
      test_id,
      label,
      COUNT(*) AS count,
      ROUND(AVG(response_time),2) AS avg,
      MEDIAN(response_time) AS median,
      MIN(response_time) AS min,
      MAX(response_time) AS max,
      QUANTILE_CONT(response_time,0.90) AS pct90,
      QUANTILE_CONT(response_time,0.95) AS pct95,
      ROUND(QUANTILE_CONT(response_time,0.99), 2) AS pct99,
      ROUND(SUM(CASE WHEN success=0 THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS error_pct,
      ROUND(COUNT(*)/(MAX(timestamp)-MIN(timestamp)+1), 2) AS throughput,
      ROUND(SUM(received_bytes)/1024/(MAX(timestamp)-MIN(timestamp)+1),2) AS received_kb_sec,
      ROUND(SUM(sent_bytes)/1024/(MAX(timestamp)-MIN(timestamp)+1),2) AS sent_kb_sec
    FROM jmeter_samples
    WHERE test_id = ?
      AND (? IS NULL OR timestamp >= ?)
      AND (? IS NULL OR timestamp <= ?)
    GROUP BY test_id, label
    ORDER BY count DESC
    """, (test_id, start, start, end, end))

@app.route("/api/aggregate")
def api_aggregate():
    test_id = request.args.get("test_id", "default")
    start = request.args.get("start", type=int)
    end = request.args.get("end", type=int)

    rows = cached_aggregate(test_id, start, end)
    keys = ["test_id","label","count","avg","median","min","max","pct90","pct95","pct99","error_pct","throughput","received_kb_sec","sent_kb_sec"]
    return jsonify([dict(zip(keys, r)) for r in rows])

# =====================================================
# TOTAL TPS
# =====================================================
@lru_cache(maxsize=128)
def cached_tps(test_id, start, end):
    q = "SELECT timestamp, COUNT(*) FROM jmeter_samples WHERE timestamp BETWEEN ? AND ?"
    p = [start, end]
    if test_id:
        q += " AND test_id=?"
        p.append(test_id)
    q += " GROUP BY timestamp ORDER BY timestamp"
    return run_query(q, tuple(p))

@app.route("/api/total_tps")
def api_total_tps():
    window = request.args.get("window", default=60, type=int)
    end = request.args.get("end", type=int) or int(time.time())
    test_id = request.args.get("test_id")

    start = end - window + 1
    bucket = choose_bucket(start, end)

    q = """
    SELECT (timestamp / ?) * ? AS bucket, COUNT(*)
    FROM jmeter_samples
    WHERE timestamp BETWEEN ? AND ?
    """
    params = [bucket, bucket, start, end]

    if test_id:
        q += " AND test_id=?"
        params.append(test_id)

    q += " GROUP BY bucket ORDER BY bucket"

    rows = run_query(q, tuple(params))
    mp = {r[0]: r[1] for r in rows}
    timestamps = list(range(start - start % bucket, end + 1, bucket))

    return jsonify({
        "timestamps": timestamps,
        "tps": [mp.get(t, 0) for t in timestamps]
    })

# =====================================================
# LABEL TPS (MULTI-LINE)
# =====================================================
@lru_cache(maxsize=64)
def cached_label_tps(test_id, start, end):
    q = """
    SELECT label, timestamp, COUNT(*)
    FROM jmeter_samples
    WHERE timestamp BETWEEN ? AND ?
    """
    p = [start, end]
    if test_id:
        q += " AND test_id=?"
        p.append(test_id)
    q += " GROUP BY label, timestamp"
    rows = run_query(q, tuple(p))

    labels = {}
    for lab, ts, cnt in rows:
        labels.setdefault(lab, {})[ts] = cnt

    return labels

@app.route("/api/label_tps")
def api_label_tps():
    window = request.args.get("window", default=60, type=int)
    end = request.args.get("end", type=int) or int(time.time())
    test_id = request.args.get("test_id")

    start = end - window + 1
    bucket = choose_bucket(start, end)

    q = """
    SELECT
      label,
      (timestamp / ?) * ? AS bucket,
      COUNT(*) AS tps
    FROM jmeter_samples
    WHERE timestamp BETWEEN ? AND ?
    """
    params = [bucket, bucket, start, end]

    if test_id:
        q += " AND test_id=?"
        params.append(test_id)

    q += " GROUP BY label, bucket ORDER BY bucket"

    rows = run_query(q, tuple(params))

    timestamps = list(range(start - start % bucket, end + 1, bucket))
    data = {}

    for label, ts, cnt in rows:
        data.setdefault(label, {})[ts] = cnt

    return jsonify({
        "timestamps": timestamps,
        "label_tps": {
            lab: [mp.get(t, 0) for t in timestamps]
            for lab, mp in data.items()
        }
    })

# =====================================================
# THREADS
# =====================================================
@lru_cache(maxsize=128)
def cached_threads(test_id, start, end):
    q = """
    SELECT timestamp, AVG(thread_count)
    FROM jmeter_samples
    WHERE timestamp BETWEEN ? AND ?
    """
    p = [start, end]
    if test_id:
        q += " AND test_id=?"
        p.append(test_id)
    q += " GROUP BY timestamp ORDER BY timestamp"
    return run_query(q, tuple(p))

@app.route("/api/threads")
def api_threads():
    window = request.args.get("window", default=60, type=int)
    end = request.args.get("end", type=int) or int(time.time())
    test_id = request.args.get("test_id")

    start = end - window + 1
    bucket = choose_bucket(start, end)

    q = """
    SELECT (timestamp / ?) * ? AS bucket, AVG(thread_count)
    FROM jmeter_samples
    WHERE timestamp BETWEEN ? AND ?
    """
    params = [bucket, bucket, start, end]

    if test_id:
        q += " AND test_id=?"
        params.append(test_id)

    q += " GROUP BY bucket ORDER BY bucket"

    rows = run_query(q, tuple(params))
    mp = {r[0]: round(r[1], 2) for r in rows}
    timestamps = list(range(start - start % bucket, end + 1, bucket))

    return jsonify({
        "timestamps": timestamps,
        "threads": [mp.get(t, 0) for t in timestamps]
    })

# =====================================================
# ERROR %
# =====================================================
@lru_cache(maxsize=128)
def cached_errorpct(test_id, start, end):
    q = """
    SELECT timestamp,
    SUM(CASE WHEN success=0 THEN 1 ELSE 0 END)*100.0/COUNT(*)
    FROM jmeter_samples
    WHERE timestamp BETWEEN ? AND ?
    """
    p = [start, end]
    if test_id:
        q += " AND test_id=?"
        p.append(test_id)
    q += " GROUP BY timestamp ORDER BY timestamp"
    return run_query(q, tuple(p))

@app.route("/api/errorpct")
def api_errorpct():
    window = request.args.get("window", default=60, type=int)
    end = request.args.get("end", type=int) or int(time.time())
    test_id = request.args.get("test_id")

    start = end - window + 1
    bucket = choose_bucket(start, end)

    q = """
    SELECT
      (timestamp / ?) * ? AS bucket,
      SUM(CASE WHEN success=0 THEN 1 ELSE 0 END)*100.0/COUNT(*)
    FROM jmeter_samples
    WHERE timestamp BETWEEN ? AND ?
    """
    params = [bucket, bucket, start, end]

    if test_id:
        q += " AND test_id=?"
        params.append(test_id)

    q += " GROUP BY bucket ORDER BY bucket"

    rows = run_query(q, tuple(params))
    mp = {r[0]: round(r[1], 2) for r in rows}
    timestamps = list(range(start - start % bucket, end + 1, bucket))

    return jsonify({
        "timestamps": timestamps,
        "error_pct": [mp.get(t, 0) for t in timestamps]
    })

# =====================================================
# ERRORS TABLE
# =====================================================
@app.route("/api/errors")
def api_errors():
    test_id = request.args.get("test_id","default")
    start = request.args.get("start",type=int)
    end = request.args.get("end",type=int)

    q = """
    SELECT label, status_code, COUNT(*), STRING_AGG(DISTINCT error_message, ' | ')
    FROM jmeter_samples
    WHERE success=0 AND test_id=?
    """
    p = [test_id]
    if start:
        q += " AND timestamp>=?"
        p.append(start)
    if end:
        q += " AND timestamp<=?"
        p.append(end)

    q += " GROUP BY label, status_code ORDER BY COUNT(*) DESC"

    rows = run_query(q,tuple(p))
    return jsonify([{
        "label":r[0],
        "status":r[1],
        "count":r[2],
        "message":r[3] or ""
    } for r in rows])

# =====================================================
# SUCCESS TABLE
# =====================================================
@app.route("/api/success")
def api_success():
    test_id = request.args.get("test_id","default")
    start = request.args.get("start",type=int)
    end = request.args.get("end",type=int)

    q = """
    SELECT label,
           COUNT(*),
           ROUND(AVG(response_time),2),
           MIN(response_time),
           MAX(response_time),
           QUANTILE_CONT(response_time,0.90)
    FROM jmeter_samples
    WHERE success=1 AND test_id=?
    """
    p = [test_id]
    if start:
        q += " AND timestamp>=?"
        p.append(start)
    if end:
        q += " AND timestamp<=?"
        p.append(end)

    q += " GROUP BY label ORDER BY COUNT(*) DESC"

    rows = run_query(q,tuple(p))
    return jsonify([{
        "label":r[0],
        "count":r[1],
        "avg":r[2],
        "min":r[3],
        "max":r[4],
        "p90":r[5]
    } for r in rows])

# =====================================================
# RESPONSE TIMES (RAW FOR LINE CHART)
# =====================================================
@app.route("/api/response_times")
def api_response_times():
    test_id = request.args.get("test_id", "default")
    start = request.args.get("start", type=int)
    end = request.args.get("end", type=int)

    if not start or not end:
        return jsonify({})

    bucket = choose_bucket(start, end)

    q = """
    SELECT
      label,
      (timestamp / ?) * ? AS bucket,
      AVG(response_time) AS avg_rt
    FROM jmeter_samples
    WHERE test_id=?
      AND timestamp BETWEEN ? AND ?
    GROUP BY label, bucket
    ORDER BY bucket
    """
    rows = run_query(q, (bucket, bucket, test_id, start, end))

    result = {}
    for label, ts, avg_rt in rows:
        result.setdefault(label, {"timestamps": [], "response_times": []})
        result[label]["timestamps"].append(ts)
        result[label]["response_times"].append(round(avg_rt, 2))

    return jsonify(result)

# =====================================================
# TEST IDS
# =====================================================
@app.route("/api/testids")
def api_testids():
    rows = run_query("SELECT DISTINCT test_id FROM jmeter_samples ORDER BY test_id")
    return jsonify([r[0] for r in rows])

# =====================================================
# DELETE TEST
# =====================================================
@app.route("/api/delete_testid",methods=["POST"])
def delete_test():
    tid = request.json.get("test_id")
    if not tid:
        return jsonify({"message":"test_id required"}),400
    with DB_LOCK:
        conn = get_db()
        CONN.execute("DELETE FROM jmeter_samples WHERE test_id=?", (tid,))
    clear_cache()
    return jsonify({"message":f"Deleted {tid}"})

# =====================================================
# CUSTOM SQL
# =====================================================
@app.route("/CustomQueryDatabase",methods=["POST"])
def custom_sql():
    q = request.json.get("query")
    try:
        with DB_LOCK:
            conn = get_db()
            cur = CONN.execute(q)
            return jsonify({
                "columns":[c[0] for c in cur.description] if cur.description else [],
                "rows":cur.fetchall()
            })
    except Exception as e:
        return jsonify({"error":str(e)})

# =====================================================
# DASHBOARD (STATIC HTML)
# =====================================================
@app.route("/")

# --------- Download snapshot HTML (hard-coded HTML file with current data embedded) ----------
@app.route("/download/snapshot.html")
def download_snapshot():
    # produce a static HTML that embeds current aggregate, errors and success as JSON so file is standalone
    agg = api_aggregate().get_json()
    errs = api_errors().get_json()
    succ = api_success().get_json()
    snapshot_html = f"""
    <!doctype html>
    <html>
    <head><meta charset="utf-8"><title>JMeter Dashboard Snapshot</title>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    </head>
    <body class="p-4">
    <h3>Snapshot taken: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</h3>
    <h4>Aggregate Report</h4>
    <pre id="agg">{json.dumps(agg, indent=2)}</pre>
    <h4>Errors</h4>
    <pre id="err">{json.dumps(errs, indent=2)}</pre>
    <h4>Success</h4>
    <pre id="succ">{json.dumps(succ, indent=2)}</pre>
    </body></html>
    """
    resp = make_response(snapshot_html)
    resp.headers["Content-Disposition"] = "attachment; filename=dashboard_snapshot.html"
    resp.headers["Content-Type"] = "text/html"
    return resp

@app.route("/dashboard", methods=["GET"])
def dashboard():
    rows = run_query("SELECT DISTINCT label FROM jmeter_samples")
    labels = sorted([r[0] for r in rows])
    return app.send_static_file("dashboard.html")
@app.route("/jmeter-dashboard.html")
def jmeter_dashboard():
    return app.send_static_file("jmeter-dashboard.html")




if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=False)
