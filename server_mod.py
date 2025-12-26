import duckdb
import threading
import time
import os
import csv
import io
from datetime import datetime
from functools import lru_cache
from flask import Flask, request, jsonify, make_response, render_template_string

# =====================================================
# CONFIG
# =====================================================
PORT = 5000
DB_DIR = "database_files"
os.makedirs(DB_DIR, exist_ok=True)

date_str = datetime.now().strftime("%d%b%Y").upper()
DB_FILE = os.path.join(DB_DIR, f"jmeter_metrics_{date_str}.duckdb")

# =====================================================
# APP & DB
# =====================================================
app = Flask(__name__)
DB_LOCK = threading.Lock()
CONN = duckdb.connect(DB_FILE)

# =====================================================
# INIT DB
# =====================================================
CURRENT_DATE = None
CONN = None

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
@app.route("/")
# --------- Dashboard UI ----------
@app.route("/dashboard")
def dashboard():
    # Build list of distinct labels for filter dropdown
    rows = run_query("SELECT DISTINCT label FROM jmeter_samples")
    labels = sorted([r[0] for r in rows])
    # serve a single big html template (kept inline for single-file simplicity)
    html = render_template_string("""
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Live Monitoring</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
  <link href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css" rel="stylesheet">
  <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/flatpickr"></script>
  <link href="https://cdn.jsdelivr.net/npm/flatpickr/dist/flatpickr.min.css" rel="stylesheet">
  <script src="https://cdn.jsdelivr.net/npm/moment@2.29.4/moment.min.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/moment-timezone@0.5.43/builds/moment-timezone-with-data.min.js"></script>
  <script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-zoom@2.0.1/dist/chartjs-plugin-zoom.min.js"></script>

  <style>
    body{padding:20px;background:#f5f7fb}
    .card{border-radius:12px;box-shadow:0 6px 18px rgba(0,0,0,0.06)}
  </style>
</head>
<body>
  <h2>Live Monitoring</h2>
  <div class="container-fluid">
    <div class="d-flex justify-content-between align-items-center mb-3">
      
      <br> 
      <div>
        <label class="me-2">Auto-refresh:</label>
        <select id="refreshSelect" class="form-select d-inline-block w-auto me-3">
          <option value="2">2s</option><option value="5" >5s</option><option value="10"selected>10s</option>
          <option value="30">30s</option><option value="60">1m</option><option value="0">Off</option>
        </select>

        <label class="me-2">Timezone:</label>
        <select id="tzSelect" class="form-select d-inline-block w-auto me-3"></select>

        <label class="me-2">Start:</label>
        <input id="startPicker" class="form-control d-inline-block w-auto me-2" placeholder="Start time">

        <label class="me-2">End:</label>
        <input id="endPicker" class="form-control d-inline-block w-auto me-2" placeholder="End time">
        <br><hr>
        <label class="me-2">Duration:</label>
        <select id="durationSelect" class="form-select d-inline-block w-auto me-2">
          <option value="">--</option>
          <option value="60">1 min</option>
          <option value="300">5 min</option>
          <option value="600" Selected>10 min</option>
          <option value="1800">30 min</option>
          <option value="3600">1 hr</option>
          <option value="7200">2 hr</option>
          <option value="10800">3 hr</option>
          <option value="21600">6 hr</option>
          <option value="43200">12 hr</option>
          <option value="86400">24 hr</option>
          <option value="172800">48 hr</option>
        </select>

        <button id="resetRange" class="btn btn-outline-secondary btn-sm">Reset</button>
        <span id="autoStatus" class="badge bg-info ms-2" style="display:none;">Auto-refresh ON</span>
        <!-- Add this inside your dashboard controls section -->
        <label for="testIdSelect" class="form-label me-2">TestId:</label>
        <select id="testIdSelect" class="form-select form-select-sm" style="width:auto;display:inline-block;">
          <!-- Options will be populated dynamically -->
        </select>

        <!-- Add for label filter -->
        <label for="labelSelect" class="form-label me-2">Label:</label>
        <select id="labelSelect" class="form-select form-select-sm" style="width:auto;display:inline-block;">
          <option value="">All</option>
        </select>
        <button id="deleteTestBtn" class="btn btn-danger" Disabled>Delete Test</button>                          
        <button id="customSqlBtn" class="btn btn-primary">CustomQery</button>
        <button id="jtltohtml" class="btn btn-success">JTL TO HTML</button>

        

        </div>
        <!-- SQL Query Modal -->
<div class="modal fade" id="customSqlModal" tabindex="-1" aria-hidden="true">
  <div class="modal-dialog modal-lg">
    <div class="modal-content bg-dark text-light">
      <div class="modal-header">
        <h5 class="modal-title">Run Custom SQL Query</h5>
        <button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal"></button>
      </div>
      <div class="modal-body">
        <textarea id="sqlQuery" class="form-control bg-dark text-light" rows="5"
          placeholder="Enter SQL query here..."></textarea>
        <button id="runSqlBtn" class="btn btn-primary mt-2">Run Query</button>
        <div id="sqlResultContainer" class="mt-3"></div>
      </div>
    </div>
  </div>
</div>

<!-- FontAwesome & Bootstrap -->
<link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>                          
    </div>

    <div class="row g-3">
      <div class="col-lg-6">
        <div class="card p-3">
          <div class="d-flex justify-content-between">
            <h5>TPS (per second)</h5>
          </div>
          <canvas id="tpsChart" height="160"></canvas>
          <div id="rangeDisplayTps" class="text-secondary small mt-2 text-end"></div>
        </div>
      </div>

      <div class="col-lg-6">
        <div class="card p-3">
          <div class="d-flex justify-content-between">
            <h5>Active Threads (per sec)</h5>
          </div>
          <canvas id="threadChart" height="160"></canvas>
          <div id="rangeDisplayThreads" class="text-secondary small mt-2 text-end"></div>
        </div>
      </div>
      <div class="col-lg-6">
        <div class="card p-3">
          <div class="d-flex justify-content-between">
            <h5>Error Percentage</h5>
          </div>
          <canvas id="errorPctChart" height="160"></canvas>
          <div id="rangeDisplayErrorPct" class="text-secondary small mt-2 text-end"></div>
        </div>
      </div>                              

      <div class="col-lg-6">
        <div class="card p-3">
          <div class="d-flex justify-content-between">
            <h5>Response Time (ms)</h5>
          </div>
          <canvas id="respTimeChart" height="160"></canvas>
          <div id="rangeDisplayRespTime" class="text-secondary small mt-2 text-end"></div>
        </div>
      </div>

      <div class="col-12">
        <div class="card p-3">
          <div class="d-flex justify-content-between mb-2">
            <h5>Aggregate Report</h5>
          </div>
          <table id="aggTable" class="table table-striped table-bordered">
  <thead class="table-dark">
    <tr>
      <th>TestId</th>
      <th>Label</th>
      <th>Count</th>
      <th>Avg</th>
      <th>Median</th>
      <th>Min</th>
      <th>Max</th>
      <th>90%</th>
      <th>95%</th>
      <th>99%</th>
      <th>Error %</th>
      <th>Throughput</th>
      <th>Received KB/sec</th>
      <th>Sent KB/sec</th>
    </tr>
  </thead>
  <tbody></tbody>
</table>
          <div id="rangeDisplayAgg" class="text-secondary small mt-2 text-end"></div>
        </div>
      </div>
                                  
      <div class="col-lg-12">
        <div class="card p-3">
          <div class="d-flex justify-content-between mb-3">
            <h5>Successful Transactions</h5>
          </div>
          <table id="succTable" class="table table-stripped table-bordered">
            <thead class="table-dark"><tr><th>Label</th><th>Count</th><th>Avg</th><th>Min</th><th>Max</th><th>90p</th></tr></thead>
            <tbody></tbody>
          </table>
          <div id="rangeDisplaySucc" class="text-secondary small mt-2 text-end"></div>
        </div>
      </div>


      <div class="col-lg-12">
        <div class="card p-3">
          <div class="d-flex justify-content-between mb-2">
            <h5>Errors</h5>
          </div>
          <table id="errTable" class="table table-stripped table-bordered">
            <thead class="table-dark"><tr><th>Label</th><th>Status</th><th>Count</th><th>Messages</th></tr></thead>
            <tbody></tbody>
          </table>
          <div id="rangeDisplayErr" class="text-secondary small mt-2 text-end"></div>
        </div>
      </div>
      
      
      <!-- Request Per Sec (All Labels) chart, full width -->
<div class="col-12">
  <div class="card p-3">
    <div class="d-flex justify-content-between">
      <h5>Request Per Sec (All Labels)</h5>
    </div>
    <canvas id="totalTpsChart" height="160"></canvas>
    <div id="rangeDisplayTotalTps" class="text-secondary small mt-2 text-end"></div>
  </div>
</div>
      
    </div>
  </div>
  <div id="loadingStatus" class="position-absolute top-0 end-0 m-3" style="z-index:1000;">
  <span class="badge bg-success" style="display:none;">Loading completed</span>
</div>
<script>
document.getElementById("jtltohtml").addEventListener("click", function () {
    window.location.href = "jmeter-dashboard.html";
});

document.getElementById('customSqlBtn').addEventListener('click', function() {
  new bootstrap.Modal(document.getElementById('customSqlModal')).show();
});

document.getElementById('runSqlBtn').addEventListener('click', async function() {
  const query = document.getElementById('sqlQuery').value;
  if (!query.trim()) return alert("Please enter an SQL query");

  const resp = await fetch('/CustomQueryDatabase', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query })
  });

  const data = await resp.json();
  const container = document.getElementById('sqlResultContainer');
  container.innerHTML = "";

  if (data.error) {
    container.innerHTML = `<div class="alert alert-danger">${data.error}</div>`;
    return;
  }

  // Build dark table
  let html = `<table class="table table-dark table-striped"><thead><tr>`;
  if (data.columns && data.columns.length > 0) {
    html += data.columns.map(c => `<th>${c}</th>`).join("");
    html += "</tr></thead><tbody>";
    data.rows.forEach(r => {
      html += "<tr>" + r.map(val => `<td>${val}</td>`).join("") + "</tr>";
    });
    html += "</tbody></table>";
  } else {
    html = "<div class='alert alert-warning'>No rows returned</div>";
  }
  container.innerHTML = html;
});
</script>

<script>
  // ----------- delete button 
  $('#deleteTestBtn').on('click', async function() {
    const testId = $('#testIdSelect').val();
    if (!testId) {
        alert("Please select a Test ID to delete.");
        return;
    }
    if (!confirm(`Are you sure you want to delete all rows for Test ID: ${testId}?`)) return;

    const resp = await fetch('/api/delete_testid', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ test_id: testId })
    });

    const result = await resp.json();
    alert(result.message);

    // Reload Test IDs after delete
    await loadTestIds();

    // Clear current selection if the deleted ID is gone
    const testIds = $('#testIdSelect option').map(function() { return this.value; }).get();
    if (!testIds.includes(testId)) {
        $('#testIdSelect').val(testIds.length > 0 ? testIds[0] : "");
    }

    // Refresh dashboard
    await refreshAll();
  });

    


                                  
  // ---------- Utilities ----------
  function epochToTZString(sec, tz, format12) {
    if(!sec) return "";
    let m = moment.unix(sec).tz(tz);
    return format12 ? m.format('YYYY-MM-DD hh:mm:ss A') : m.format('YYYY-MM-DD HH:mm:ss');
  }

  function tableToCSV($table) {
    var data = [];
    $table.find('tr').each(function() {
      var row = [];
      $(this).find('th,td').each(function() {
        row.push('"' + $(this).text().replace(/"/g,'""').trim() + '"');
      });
      data.push(row.join(','));
    });
    return data.join('\\n');
  }


  // ---------- Controls ----------
  const tzs = moment.tz.names();
  tzs.forEach(t => { $('#tzSelect').append(new Option(t, t)); });
  $('#tzSelect').val(moment.tz.guess());

  flatpickr("#startPicker", { enableTime:true, time_24hr:false, dateFormat:"Y-m-d h:i K" });
  flatpickr("#endPicker", { enableTime:true, time_24hr:false, dateFormat:"Y-m-d h:i K" });

  // ---------- Charts ----------
  let errorPctChart = new Chart(document.getElementById('errorPctChart'), {
    type: 'line',
    data: {
        labels: [],
        datasets: [{
        label: 'Error %',
        data: [],
        borderColor: 'red',
        backgroundColor: 'rgba(255,0,0,0.2)',
        borderWidth: 2,
        tension: 0.3
        }]
    },
    options: {
        scales: {
        x: { ticks: { maxRotation: 45 } },
        y: {
            beginAtZero: true,
            title: { display: true, text: "%" }
        }
        }
    }
    });
                                
  let tpsChart = new Chart(document.getElementById('tpsChart'), {
    type:'line',
    data:{
        labels:[],
        datasets:[{
        label:'TPS',
        data:[],
        tension:0.3,
        borderColor: 'blue',        // line color
        borderWidth: 3,             // line thickness
        pointRadius: 2,             // point size
        backgroundColor: 'rgba(0,0,255,0.1)' // optional fill under line
        }]
    }
    });
  let threadChart = new Chart(document.getElementById('threadChart'), {
  type:'line',
  data:{
    labels:[],
    datasets:[{
      label:'Threads',
      data:[],
      tension:0.3,
      borderColor: 'green',       // line color
      borderWidth: 2,             // line thickness
      pointRadius: 2,
      backgroundColor: 'rgba(0,255,0,0.1)'
    }]
  }
});
let respTimeChart = new Chart(document.getElementById('respTimeChart'), {
  type: 'line',
  data: {
    labels: [],
    datasets: [{
      label: 'Response Time',
      data: [],
      borderColor: 'purple',
      backgroundColor: 'rgba(128,0,128,0.1)',
      borderWidth: 2,
      tension: 0.3
    }]
  },
  options: {
    scales: {
      x: { ticks: { maxRotation: 45 } },
      y: {
        beginAtZero: true,
        title: { display: true, text: "ms" }
      }
    }
  }
});
let totalTpsChart = new Chart(document.getElementById('totalTpsChart'), {
  type: 'line',
  data: {
    labels: [],
    datasets: [{
      label: 'Total TPS',
      data: [],
      borderColor: 'orange',
      backgroundColor: 'rgba(255,165,0,0.1)',
      borderWidth: 3,
      tension: 0.3,
      pointRadius: 2
    }]
  }
});

  // ---------- Data Params ----------
  function getRangeParams() {
    let tz = $('#tzSelect').val();
    let duration = $('#durationSelect').val();
    let startRaw = $('#startPicker').val();
    let endRaw = $('#endPicker').val();
    let start = null, end = null;

    if (duration) {
        // Duration mode: ignore start/end pickers
        end = moment().unix();
        start = end - parseInt(duration);
        return { start, end, tz, duration: parseInt(duration), mode: 'duration' };
    } else if (startRaw || endRaw) {
        // Start/end mode: ignore duration
        if (startRaw) start = moment.tz(startRaw, 'YYYY-MM-DD hh:mm:ss A', tz).unix();
        if (endRaw) end = moment.tz(endRaw, 'YYYY-MM-DD hh:mm:ss A', tz).unix();
        // If only start is set, default end to now
        if (start && !end) end = moment().unix();
        return { start, end, tz, duration: null, mode: 'range' };
    } else {
        // No filters: show all time
        return { start: null, end: null, tz, duration: null, mode: 'all' };
    }
  }
    function getRangeParamsNew() {
    const start = $('#startTime').val() ? Math.floor(new Date($('#startTime').val()).getTime()/1000) : null;
    const end   = $('#endTime').val() ? Math.floor(new Date($('#endTime').val()).getTime()/1000) : null;
    const duration = $('#durationSelect').val() ? parseInt($('#durationSelect').val()) : null;
    return { start, end, duration };
    }


  // ---------- Loaders ----------
    async function loadErrorPct_notused() {
    const {start, end} = getRangeParams();
    const testId = $('#testIdSelect').val();                              
    let window = 60;
    if (start && end) {
        window = Math.max(60, end - start + 1);
    }
    let url ='/api/errorpct?window=' + window + (end ? ('&end=' + end) : ''); 
    if (testId) url += '&test_id=' + encodeURIComponent(testId);                                                           
    const resp = await fetch(url);
                                  
    const data = await resp.json();
    const tz = $('#tzSelect').val();
    let labels = data.timestamps.map(s => moment.unix(s).tz(tz).format('HH:mm:ss A'));
    errorPctChart.data.labels = labels;
    errorPctChart.data.datasets[0].data = data.error_pct;
    errorPctChart.update();
    }

  
// ...existing code...
// ...existing code...
async function loadTotalTPS() {
    const { start, end, duration, mode } = getRangeParams();
    let window = 60;
    if (mode === 'duration') {
        window = duration;
    } else if (mode === 'range') {
        window = Math.max(60, end - start + 1);
    }
    const testId = $('#testIdSelect').val();
    let url = '/api/label_tps?window=' + window + (end ? ('&end=' + end) : '');
    if (testId) url += '&test_id=' + encodeURIComponent(testId);

    const resp = await fetch(url);
    const data = await resp.json();
    const tz = $('#tzSelect').val();
    const labels = data.timestamps.map(s => moment.unix(s).tz(tz).format('HH:mm:ss A'));

    // Build datasets for each label
    const datasets = [];
    let colorIdx = 0;
    const colors = ['#e74c3c', '#3498db', '#2ecc71', '#f39c12', '#9b59b6', '#1abc9c', '#34495e', '#95a5a6'];
    for (const [label, tpsArr] of Object.entries(data.label_tps)) {
        datasets.push({
            label: label,
            data: tpsArr,
            borderColor: colors[colorIdx % colors.length],
            backgroundColor: colors[colorIdx % colors.length] + '33',
            borderWidth: 1,
            tension: 0.3,
            pointRadius: 0,
            spanGaps:true
                                  
        });
        colorIdx++;
    }

    totalTpsChart.data.labels = labels;
    totalTpsChart.data.datasets = datasets;
    totalTpsChart.update();
}
// ...existing code... 
async function loadTPS() {
  const { start, end, duration, mode } = getRangeParams();
  const testId = $('#testIdSelect').val();
  let url = '/api/total_tps?';
  if (mode === 'duration') {
    url += 'window=' + duration + '&end=' + end;
  } else if (mode === 'range') {
    url += 'window=' + Math.max(60, end - start + 1) + '&end=' + end;
  } else {
    url += 'window=60';
  }
  if (testId) url += '&test_id=' + encodeURIComponent(testId);
  const resp = await fetch(url);
  const data = await resp.json();
  const tz = $('#tzSelect').val();
  tpsChart.data.labels = data.timestamps.map(s => moment.unix(s).tz(tz).format('HH:mm:ss A'));
  // Always fill missing values with 0
 tpsChart.data.datasets[0] = {
  label: 'TPS',
  data: data.tps.map(v =>
    (typeof v === 'number' && !isNaN(v) && v !== 0) ? v : null
  ),
  borderColor: 'blue',
  backgroundColor: 'rgba(0,0,255,0.1)',
  borderWidth: 1,
  tension: 0.3,
  pointRadius: 0,
  spanGaps: false  // connects over nulls
};

  tpsChart.update();
}
// ...existing code...
async function loadThreads() {
  const { start, end } = getRangeParams();
  const testId = $('#testIdSelect').val();

  let window = (start && end) ? Math.max(60, end - start + 1) : 60;
  let url = '/api/threads?window=' + window + (end ? ('&end=' + end) : '');
  if (testId) url += '&test_id=' + encodeURIComponent(testId);

  const resp = await fetch(url);
  const data = await resp.json();
  const tz = $('#tzSelect').val();

  // Labels: match timestamps from backend
  threadChart.data.labels = data.timestamps.map(s =>
    moment.unix(s).tz(tz).format('HH:mm:ss A')
  );

  // Values: keep numbers, null for missing, force start/end to 0
  const values = data.threads.map(v =>
    (typeof v === 'number' && !isNaN(v) && v !== 0) ? v : null
  );
  if (values.length > 0) {
    values[0] = 0;                           // force start
    values[values.length - 1] = 0;           // force end
  }

  threadChart.data.datasets[0] = {
    label: testId ? 'Threads' : 'Threads',
    data: values,
    borderColor: 'green',
    backgroundColor: 'rgba(0,255,0,0.1)',
    borderWidth: 2,
    tension: 0.4,
    pointRadius: 0,
    spanGaps: false   // line will break at nulls, drop to 0 at ends
  };

  threadChart.update();
}


async function loadErrorPct() {
  const { start, end } = getRangeParams();
  const testId = $('#testIdSelect').val();
  let window = 60;
  if (start && end) {
    window = Math.max(60, end - start + 1);
  }
  let url = '/api/errorpct?window=' + window + (end ? ('&end=' + end) : '');
  if (testId) url += '&test_id=' + encodeURIComponent(testId);

  const resp = await fetch(url);
  const data = await resp.json();
  const tz = $('#tzSelect').val();
  errorPctChart.data.labels = data.timestamps.map(s =>
    moment.unix(s).tz(tz).format('HH:mm:ss A')
  );
  errorPctChart.data.datasets[0] = {
    label: 'Error %',
    data: data.error_pct.map(v => v || 0),
    borderColor: 'red',
    backgroundColor: 'rgba(255,0,0,0.2)',
    borderWidth: 1,
    tension: 0.3,
    pointRadius: 0,
    spanGaps: false
  };
  errorPctChart.update();
}


async function loadRespTime() {
  const { start, end, tz } = getRangeParams();
  const testId = $('#testIdSelect').val();
  const params = new URLSearchParams();
  if (testId) params.append('test_id', testId);
  if (start) params.append('start', start);
  if (end) params.append('end', end);

  const resp = await fetch('/api/response_times?' + params.toString());
  const data = await resp.json();

  // Find the union of all timestamps
  let allTimestamps = [];
  for (const labelData of Object.values(data)) {
    if (labelData.timestamps) {
      allTimestamps = allTimestamps.concat(labelData.timestamps);
    }
  }
  allTimestamps = Array.from(new Set(allTimestamps)).sort((a, b) => a - b);

  const labels = allTimestamps.map(s => moment.unix(s).tz(tz).format('HH:mm:ss A'));
  respTimeChart.data.labels = labels;
  respTimeChart.data.datasets = [];

  const colors = ['#e74c3c','#3498db','#2ecc71','#f39c12','#9b59b6','#1abc9c','#34495e','#95a5a6'];
  let colorIdx = 0;

  for (const [label, values] of Object.entries(data)) {
    const timeMap = {};
    if (values.timestamps && values.response_times) {
      values.timestamps.forEach((t, i) => { timeMap[t] = values.response_times[i]; });
    }

    // Align to union of timestamps
    let alignedRespTimes = allTimestamps.map(t =>
      (timeMap[t] !== undefined && timeMap[t] !== 0) ? timeMap[t] : null
    );

    // Force start and end to 0
    if (alignedRespTimes.length > 0) {
      alignedRespTimes[0] = 0;
      alignedRespTimes[alignedRespTimes.length - 1] = 0;
    }

    respTimeChart.data.datasets.push({
      label: label,
      data: alignedRespTimes,
      borderColor: colors[colorIdx % colors.length],
      backgroundColor: colors[colorIdx % colors.length] + '22',
      borderWidth: 2,
      tension: 0.4,
      pointRadius: 0,
      spanGaps: false   // break lines at nulls
    });
    colorIdx++;
  }

  respTimeChart.update();
  $('#rangeDisplayRespTime').text($('#rangeDisplayAgg').text());
}

// ...existing code...


    function getTestId() {
    return $('#testIdSelect').val();
    }

    async function loadAggregate() {
    const { start, end } = getRangeParams();
    const testId = getTestId();
    const params = new URLSearchParams();
    params.append('test_id', testId);
    if (start) params.append('start', start);
    if (end) params.append('end', end);

    const resp = await fetch('/api/aggregate?' + params.toString());
    const data = await resp.json();

    // Initialize DataTable only once
    let table;
    if (!$.fn.dataTable.isDataTable('#aggTable')) {
        table = $('#aggTable').DataTable({ order: [[2, "desc"]], pageLength: 10 });
    } else {
        table = $('#aggTable').DataTable();
        table.clear();
    }

    // Use DataTables API to add rows
    data.forEach(r => {
      table.row.add([
        r.test_id,
        r.label,
        r.count,
        r.avg,
        r.median,
        r.min,
        r.max,
        r.pct90,
        r.pct95,
        r.pct99,
        r.error_pct,
        r.throughput,
        r.received_kb_sec,
        r.sent_kb_sec
      ]);
    });
    table.draw();
}
                                

  async function loadErrors() {
  const {start,end} = getRangeParams();
  const testId = getTestId();
  let url = '/api/errors?test_id=' + encodeURIComponent(testId) + '&';
  if(start) url += 'start=' + start + '&';
  if(end) url += 'end=' + end + '&';
  const data = await (await fetch(url)).json();

  // Initialize DataTable only once
  let table;
  if (!$.fn.dataTable.isDataTable('#errTable')) {
    table = $('#errTable').DataTable({ pageLength: 10 });
  } else {
    table = $('#errTable').DataTable();
    table.clear();
  }
  data.forEach(r => {
    table.row.add([r.label, r.status, r.count, r.message||'']);
  });
  table.draw();
}

async function loadSuccess() {
  const {start,end} = getRangeParams();
  const testId = getTestId();
  let url = '/api/success?test_id=' + encodeURIComponent(testId) + '&';
  if(start) url += 'start=' + start + '&';
  if(end) url += 'end=' + end + '&';
  const data = await (await fetch(url)).json();

  // Initialize DataTable only once
  let table;
  if (!$.fn.dataTable.isDataTable('#succTable')) {
    table = $('#succTable').DataTable({ pageLength: 10 });
  } else {
    table = $('#succTable').DataTable();
    table.clear();
  }
  data.forEach(r => {
    table.row.add([r.label, r.count, r.avg, r.min, r.max, r.p90]);
  });
  table.draw();
}

  async function refreshAll() {
    $('#loadingStatus span').hide();
    updateRangeDisplays();                                
    await Promise.all([
  loadTPS(), loadThreads(), loadErrorPct(), loadAggregate(),
  loadErrors(), loadSuccess(), loadRespTime(), loadTotalTPS(),loadTPS()
  ]);
    setAutoRefresh(parseInt($('#refreshSelect').val()));
    $('#loadingStatus span').show();
    setTimeout(() => { $('#loadingStatus span').fadeOut(); }, 2000); // Hide after 2 seconds
  }


    function enforceRefreshByDuration() {
    const duration = parseInt($('#durationSelect').val());
    const refreshSelect = $('#refreshSelect');

    // Duration > 12 hours (43200 seconds)
    if (duration && duration > 43200) {
        refreshSelect.val('5');        // force 5s
        refreshSelect.prop('disabled', true);
        setAutoRefresh(5);
    } else {
        refreshSelect.prop('disabled', false);
        setAutoRefresh(parseInt(refreshSelect.val()));
    }
    }

  // ---------- Auto-refresh ----------
  let autoHandle = null;
  function setAutoRefresh(seconds) {
  if (autoHandle) {
    clearInterval(autoHandle);
    autoHandle = null;
  }

  if (seconds > 0) {
    $('#autoStatus').show();
    autoHandle = setInterval(refreshAll, seconds * 1000);
  } else {
    $('#autoStatus').hide();
  }
}

  setAutoRefresh(parseInt($('#refreshSelect').val()));

  // ---------- Events ----------
  $('#refreshSelect').on('change', function(){ setAutoRefresh(parseInt($(this).val())); });
  $('#labelFilter').on('change', loadAggregate);
  $('#startPicker,#endPicker,#durationSelect').on('change', refreshAll);
  $('#startPicker,#endPicker').on('change', function() {
    if ($('#startPicker').val() || $('#endPicker').val()) {
        $('#durationSelect').val('');
    }
});
  $('#resetRange').on('click', function(){
    $('#startPicker').val(''); $('#endPicker').val(''); $('#durationSelect').val('');
    refreshAll();
  });

  // Copy & download table/chart handlers unchanged...
  function enableChartButtons(chart, copyBtnId, downloadBtnId, filename) {
    $(copyBtnId).on('click', async function(){
      try {
        const url = chart.toBase64Image();
        const res = await fetch(url);
        const blob = await res.blob();
        await navigator.clipboard.write([new ClipboardItem({[blob.type]: blob})]);
        alert('Chart image copied to clipboard (if supported by browser).');
      } catch { $(downloadBtnId).click(); }
    });
    $(downloadBtnId).on('click', function(){
      const a = document.createElement('a');
      a.href = chart.toBase64Image(); a.download = filename;
      document.body.appendChild(a); a.click(); a.remove();
    });
  }
  enableChartButtons(tpsChart, '#copyTps', '#downloadTpsPng', 'tps.png');
  enableChartButtons(threadChart, '#copyThreads', '#downloadThreadsPng', 'threads.png');

  // ---------- Range Display Update ----------
  function updateRangeDisplays() {
    const { start, end, tz } = getRangeParams();
    let startStr = start ? epochToTZString(start, tz, true) : '';
    let endStr = end ? epochToTZString(end, tz, true) : '';
    let rangeText = '';
    if (startStr && endStr) {
        rangeText = `${startStr} to ${endStr}`;
    } else if (startStr) {
        rangeText = `${startStr} to Now`;
    } else if (endStr) {
        rangeText = `Up to ${endStr}`;
    } else {
        rangeText = 'All Time';
    }
    $('#rangeDisplayAgg').text(rangeText);
    $('#rangeDisplayTps').text(rangeText);
    $('#rangeDisplayErr').text(rangeText);
    $('#rangeDisplaySucc').text(rangeText);
    $('#rangeDisplayThreads').text(rangeText);
    $('#rangeDisplayErrorPct').text(rangeText);
    $('#rangeDisplayRespTime').text(rangeText);
    $('#rangeDisplayTotalTps').text(rangeText);
}

  // Initial load
  $(document).ready(function(){ refreshAll(); setTimeout(refreshAll, 1000); });
  async function loadTestIds() {
    const resp = await fetch('/api/testids');
    const testIds = await resp.json();
    const sel = $('#testIdSelect').empty();
    testIds.forEach(id => {
        sel.append(`<option value="${id}">${id}</option>`);
    });
    refreshAll(); 
    // If no test IDs exist, keep button disabled
    if (testIds.length === 0) {
        $('#deleteTestBtn').prop('disabled', true);
    } else {
        $('#deleteTestBtn').prop('disabled', false);
    } 
                                                              
}
$(document).ready(function() {
    loadTestIds();
    // ...other init code...
});
// ...existing code...

function movingAverage(arr, windowSize) {
    let result = [];
    for (let i = 0; i < arr.length; i++) {
        let start = Math.max(0, i - windowSize + 1);
        let window = arr.slice(start, i + 1);
        let avg = window.reduce((a, b) => a + b, 0) / window.length;
        result.push(Number(avg.toFixed(2)));
    }
    return result;
}

// In your loadTotalTPS, after fetching data:
for (const [label, tpsArr] of Object.entries(data.label_tps)) {
    const smoothed = movingAverage(tpsArr, 10); // 10-second window
    datasets.push({
        label: label,
        data: smoothed,
        // ...colors etc...
    });
}                                                                   
</script>

</body>
</html>
    """, labels=labels)
    return html
@app.route("/jmeter-dashboard.html")
def jmeter_dashboard():
    return app.send_static_file("jmeter-dashboard.html")
# =====================================================
# RUN
# =====================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=False)
