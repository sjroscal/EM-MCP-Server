"""
Oracle Enterprise Manager MCP Server
======================================
Uses mcp.server.fastmcp.FastMCP (MCP SDK built-in) — NOT the 'fastmcp' package.

Design philosophy — hybrid SQL + REST:
  • SQL via REST (executesql endpoint) for all inventory and metric queries.
    One SQL round trip replaces hundreds of per-target REST calls.
    mgmt$metric_current  -> latest snapshot  (fastest, 1 query)
    mgmt$metric_details  -> time-series history (1 query with GROUP BY)
  • Native metric REST API (metricTimeSeries) only where SQL cannot help,
    e.g. real-time sub-minute data or metrics not stored in SYSMAN.

API call count comparison:
  get_database_utilization (30 DBs):
    Old REST approach : 1 + (30 × 18) = 541 calls
    New SQL approach  : 2 calls (current + history)

Config — .env file search order:
  1. ENV_FILE env var  (explicit override)
  2. ./config/.env
  3. ./.env

.env template:
  EM_BASE_URL=https://111.111.111.111:7803/em
  EM_USER=emmcpuser
  EM_PASSWORD=YourPasswordHere
  EM_SSL_VERIFY=false
  EM_TIMEOUT=60
  MCP_PORT=8090
  MCP_TRANSPORT=sse
  LOG_LEVEL=INFO

Transport:
  streamable-http -> http://0.0.0.0:MCP_PORT/mcp
  stdio           -> Claude Desktop direct

OMS prerequisite (one-time, enables SQL via REST):
  emctl set property \
    -name oracle.sysman.db.restfulapi.executesql.repository.query.enable \
    -value true -sysman_pwd <pwd>
  emctl stop oms -all && emctl start oms
"""

import os, re, sys, json, logging
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import httpx
from mcp.server.fastmcp import FastMCP

# ─────────────────────────────────────────────────────────────────────────────
# .env + logging + config
# ─────────────────────────────────────────────────────────────────────────────
def _load_env() -> Path:
    script_dir = Path(__file__).parent.resolve()
    for path in [Path(os.getenv("ENV_FILE", "")),
                 script_dir / "config" / ".env",
                 script_dir / ".env"]:
        if path and path.is_file():
            load_dotenv(dotenv_path=path, override=False)
            return path
    return None

_env_file = _load_env()

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("em_mcp")
logger.info("Config: %s", _env_file or "env vars only")

EM_BASE_URL   = os.getenv("EM_BASE_URL",   "https://mel01em:7803/em")
EM_USER       = os.getenv("EM_USER",       "sysman")
EM_PASSWORD   = os.getenv("EM_PASSWORD",   "welcome1")
EM_SSL_VERIFY = os.getenv("EM_SSL_VERIFY", "false").lower() == "true"
EM_CA_BUNDLE  = os.getenv("EM_CA_BUNDLE",  "")
EM_TIMEOUT    = float(os.getenv("EM_TIMEOUT", "60"))
EM_ZDLRA_AUTHORIZATION = os.getenv("EM_ZDLRA_AUTHORIZATION", "").strip()
EM_ZDLRA_TARGET_ID = os.getenv("EM_ZDLRA_TARGET_ID", "").strip()
MCP_PORT      = int(os.getenv("MCP_PORT", "8090"))
MCP_TRANSPORT = os.getenv("MCP_TRANSPORT", "sse")

REPO_SQL_PATH = ("/websvcs/restful/emws/oracle.sysman.db"
                 "/executesql/repository/query/v1")

# ─────────────────────────────────────────────────────────────────────────────
# Target type aliases  (confirmed from actual mgmt$target)
# ─────────────────────────────────────────────────────────────────────────────
_TARGET_TYPES = {
    # host
    "host": "host",
    # database / CDB / instance
    "database": "oracle_database", "db": "oracle_database",
    "cdb": "oracle_database", "instance": "oracle_database",
    "oracle_database": "oracle_database",
    # PDB
    "pdb": "oracle_pdb", "oracle_pdb": "oracle_pdb",
    # RAC
    "rac": "rac_database", "rac_database": "rac_database",
    "rac database": "rac_database", "cluster_database": "rac_database",
    "cluster database": "rac_database",
    # DB system
    "dbsys": "oracle_dbsys", "oracle_dbsys": "oracle_dbsys",
    # cluster / HAS
    "cluster": "cluster", "has": "has",
    # home / listener
    "home": "oracle_home", "oracle_home": "oracle_home",
    "listener": "oracle_listener", "oracle_listener": "oracle_listener",
    # ZDLRA  (confirmed: oracle_dblra)
    "zdlra": "oracle_dblra", "dblra": "oracle_dblra",
    "oracle_dblra": "oracle_dblra", "recovery appliance": "oracle_dblra",
    # Exadata on-prem
    "exadata": "oracle_exadata", "oracle_exadata": "oracle_exadata",
    "exadata machine": "oracle_dbmachine", "dbmachine": "oracle_dbmachine",
    "exadata_grid": "oracle_exadata_grid", "exadata grid": "oracle_exadata_grid",
    "exadata_dbsys": "oracle_exadata_dbsys",
    # Exadata Cloud / VM Cluster  (confirmed: oracle_cloud_vm_cluster)
    "exadata_cloud": "oracle_cloud_vm_cluster",
    "exadata cloud": "oracle_cloud_vm_cluster",
    "vm_cluster": "oracle_cloud_vm_cluster",
    "vm cluster": "oracle_cloud_vm_cluster",
    "vmcluster": "oracle_cloud_vm_cluster",
    "exadb": "oracle_cloud_vm_cluster",
    "exadb-xs": "oracle_cloud_vm_cluster",
    "oracle_cloud_vm_cluster": "oracle_cloud_vm_cluster",
    # Exascale  (confirmed: oracle_exascale)
    "exascale": "oracle_exascale", "oracle_exascale": "oracle_exascale",
    # generic cloud
    "cloud": "oracle_cloud", "oracle_cloud": "oracle_cloud",
}

def _resolve_type(t: str) -> str:
    if not t:
        return t
    key = t.lower().strip()
    return (_TARGET_TYPES.get(key)
            or _TARGET_TYPES.get(key.replace(" ", "_"))
            or _TARGET_TYPES.get(key.replace("_", " "))
            or t)

# ─────────────────────────────────────────────────────────────────────────────
# Time window parser
# Returns (start_iso, end_iso, rollup)
# rollup: raw (<=2h) | hourly (<=24h) | daily (>24h)
# Override with suffix:  "6h:raw"  "last_7_days:hourly"
# ─────────────────────────────────────────────────────────────────────────────
def _time_range(window: str) -> tuple:
    """
    Accepted formats:
      Named  : daily, last_3_days, last_7_days, last_month
      Minutes: 20m, 30m, 90m
      Hours  : 1h, 3h, 6h, 12h, 48h
      Days   : 2d, 7d, 30d
      Rollup override suffix: 6h:raw | last_7_days:hourly | daily:daily
    """
    now = datetime.now(timezone.utc)
    fmt = "%Y-%m-%dT%H:%M:%S.000Z"
    rollup_override = None

    if ":" in window:
        window, rollup_override = window.rsplit(":", 1)
        rollup_override = rollup_override.lower().strip()

    presets = {"daily": timedelta(days=1), "last_3_days": timedelta(days=3),
               "last_7_days": timedelta(days=7), "last_month": timedelta(days=30)}
    key = window.strip().lower()

    if key in presets:
        delta = presets[key]
    else:
        m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*(h|d|m)", key)
        if m:
            v, u = float(m.group(1)), m.group(2)
            delta = {"h": timedelta(hours=v), "d": timedelta(days=v),
                     "m": timedelta(minutes=v)}[u]
        else:
            try:
                delta = timedelta(hours=float(key))
            except ValueError:
                raise ValueError(f"Unknown time_window '{window}'. "
                                 "Use: daily, 6h, 20m, 3d, or add :raw/:hourly/:daily")

    mins = delta.total_seconds() / 60
    if rollup_override in ("raw", "hourly", "daily"):
        rollup = rollup_override
    elif mins <= 120:
        rollup = "raw"
    elif mins <= 1440:
        rollup = "hourly"
    else:
        rollup = "daily"

    start = now - delta
    return start.strftime(fmt), now.strftime(fmt), rollup

# ─────────────────────────────────────────────────────────────────────────────
# HTTP + SQL helpers
# ─────────────────────────────────────────────────────────────────────────────
def _client() -> httpx.Client:
    ssl = EM_CA_BUNDLE if (EM_SSL_VERIFY and EM_CA_BUNDLE) else EM_SSL_VERIFY
    return httpx.Client(base_url=EM_BASE_URL, auth=(EM_USER, EM_PASSWORD),
                        verify=ssl, timeout=EM_TIMEOUT,
                        headers={"Accept": "application/json",
                                 "Content-Type": "application/json"})

def _sql(query: str, max_rows: int = 500) -> list:
    """
    Execute SQL against SYSMAN repository via REST. Returns list[dict] (UPPERCASE keys).
    Single API call regardless of result size.
    """
    with _client() as c:
        r = c.post(REPO_SQL_PATH, json={"sqlStatement": query.strip(),
                                         "maxRowLimit": max_rows,
                                         "maxColumnLimit": 60})
        r.raise_for_status()
        data = r.json()

    # EM 24ai returns {"Result": [...]}; older EM returns {"items": [...]}
    raw = data.get("Result") or data.get("items") or []
    if not raw:
        # Array-style: {"columns": [...], "rows": [...]}
        cols = [c["name"].upper() if isinstance(c, dict) else str(c).upper()
                for c in data.get("columns", [])]
        raw = [dict(zip(cols, row)) for row in data.get("rows", [])]
        return raw
    return [{k.upper(): v for k, v in row.items()} for row in raw]

def _v(row: dict, *keys):
    """Case-insensitive value lookup."""
    for k in keys:
        for c in (k.upper(), k.lower(), k):
            if c in row:
                return row[c]
    return None

def _err(exc: Exception) -> str:
    if isinstance(exc, httpx.HTTPStatusError):
        return json.dumps({"error": f"EM API {exc.response.status_code}: "
                                    f"{exc.response.text[:300]}"})
    return json.dumps({"error": str(exc)})


def _em_request_json(method: str, path: str, *, params: dict | None = None,
                     headers: dict | None = None) -> dict:
    """Execute an EM REST call and return parsed JSON."""
    with _client() as c:
        r = c.request(method, path, params=params, headers=headers)
        r.raise_for_status()
        try:
            return r.json()
        except ValueError as exc:
            raise RuntimeError(f"EM API returned non-JSON response: {r.text[:300]}") from exc


def _extract_page_token(next_page_obj) -> str | None:
    """Normalize EM pagination links into a page token string."""
    if not next_page_obj:
        return None
    if isinstance(next_page_obj, str):
        token = next_page_obj.strip()
        if token.startswith('http://') or token.startswith('https://') or token.startswith('/'):
            from urllib.parse import urlparse, parse_qs
            parsed = urlparse(token)
            token = parse_qs(parsed.query).get('page', [None])[0]
        return token or None
    if isinstance(next_page_obj, dict):
        href = next_page_obj.get('href') or next_page_obj.get('url') or next_page_obj.get('link')
        if href:
            from urllib.parse import urlparse, parse_qs
            parsed = urlparse(str(href))
            token = parse_qs(parsed.query).get('page', [None])[0]
            return token or None
    return None


def _resolve_zdlra_target_id(zdlra_name: str = "") -> tuple[str | None, str | None]:
    """Resolve a ZDLRA target GUID from EM repository metadata."""
    clause = (f"AND UPPER(t.target_name) LIKE UPPER('%{zdlra_name}%')"
              if zdlra_name else "")
    rows = _sql(f"""
        SELECT t.target_guid, t.target_name
        FROM sysman.mgmt$target t
        WHERE t.target_type = 'oracle_dblra'
          {clause}
        ORDER BY t.target_name
    """, max_rows=50)
    if not rows:
        return None, None
    row = rows[0]
    return str(_v(row, 'TARGET_GUID') or '').strip() or None, str(_v(row, 'TARGET_NAME') or '').strip() or None

# ─────────────────────────────────────────────────────────────────────────────
# Startup
# ─────────────────────────────────────────────────────────────────────────────
def _validate_config():
    if not EM_PASSWORD:
        logger.error("EM_PASSWORD not set — add to .env or export")
        sys.exit(1)
    logger.info("EM=%s  user=%s  port=%s  transport=%s",
                EM_BASE_URL, EM_USER, MCP_PORT, MCP_TRANSPORT)

# ─────────────────────────────────────────────────────────────────────────────
# MCP server
# ─────────────────────────────────────────────────────────────────────────────
mcp = FastMCP("Oracle EM MCP Server", host="0.0.0.0", port=MCP_PORT)


# ═════════════════════════════════════════════════════════════════════════════
# Tool 1 — ping_em_repository
# API calls: 1 SQL
# ═════════════════════════════════════════════════════════════════════════════
@mcp.tool()
def ping_em_repository() -> str:
    """
    Test connectivity to Oracle EM and SYSMAN repository.
    Returns EM version, target counts, and current DB time.
    No arguments required.
    """
    try:
        rows = _sql("""
            SELECT
                (SELECT version FROM sysman.mgmt_versions
                  WHERE component_name='CORE' AND rownum=1) AS em_version,
                (SELECT count(*) FROM sysman.mgmt$target)   AS total_targets,
                (SELECT count(*) FROM sysman.mgmt$target
                  WHERE target_type='host')                 AS total_hosts,
                (SELECT count(*) FROM sysman.mgmt$target
                  WHERE target_type IN (
                    'oracle_database','rac_database','oracle_pdb'))
                                                            AS total_databases,
                TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS')    AS db_time
            FROM dual
        """, max_rows=1)
        row = rows[0] if rows else {}
        return json.dumps({"status": "OK", "em_url": EM_BASE_URL,
                           "em_version":      _v(row, "EM_VERSION"),
                           "total_targets":   _v(row, "TOTAL_TARGETS"),
                           "total_hosts":     _v(row, "TOTAL_HOSTS"),
                           "total_databases": _v(row, "TOTAL_DATABASES"),
                           "db_time":         _v(row, "DB_TIME")}, indent=2)
    except Exception as exc:
        return _err(exc)


# ═════════════════════════════════════════════════════════════════════════════
# Tool 2 — list_targets
# API calls: 1 SQL
# ═════════════════════════════════════════════════════════════════════════════
@mcp.tool()
def list_targets(target_type: str = "", name_filter: str = "") -> str:
    """
    List monitored targets in Oracle EM with availability status.

    Args:
        target_type: Optional type filter. Aliases accepted:
                     host, database, cdb, pdb, rac, zdlra,
                     exadata, exadata_cloud, exadb, exascale,
                     vm_cluster, dbsys, cluster, listener, home.
                     Leave blank for all types.
        name_filter: Optional substring on target name (case-insensitive).

    Returns:
        JSON list with name, type, host, status.  1 SQL call.
    """
    clauses = []
    if target_type:
        clauses.append(f"AND t.target_type = '{_resolve_type(target_type)}'")
    if name_filter:
        clauses.append(f"AND UPPER(t.target_name) LIKE UPPER('%{name_filter}%')")

    try:
        rows = _sql(f"""
            SELECT t.target_name, t.target_type, t.host_name,
                   avail.availability_status AS status
            FROM   sysman.mgmt$target t
            LEFT JOIN sysman.mgmt$availability_current avail
                   ON avail.target_guid = t.target_guid
            WHERE  1=1 {" ".join(clauses)}
            ORDER  BY t.target_type, t.target_name
        """)
        result = [{"name":   _v(r, "TARGET_NAME"),
                   "type":   _v(r, "TARGET_TYPE"),
                   "host":   _v(r, "HOST_NAME"),
                   "status": _v(r, "STATUS")} for r in rows]
        return json.dumps({"count": len(result), "targets": result}, indent=2)
    except Exception as exc:
        return _err(exc)


# ═════════════════════════════════════════════════════════════════════════════
# Tool 3 — get_host_cpu_utilization
# API calls: 1 SQL  (replaces N sequential REST calls)
# ═════════════════════════════════════════════════════════════════════════════
@mcp.tool()
def get_host_cpu_utilization(
    host_name:   str = "",
    time_window: str = "daily",
) -> str:
    """
    CPU and memory utilisation for host targets.
    Single SQL query against SYSMAN — no per-host REST loops.

    Metrics returned (from mgmt$metric_details / mgmt$metric_current):
      cpuUtil       -> CPU Utilization (%)
      memUsedPct    -> Memory Utilization (%)
      swapUtil      -> Swap Utilization (%)

    Args:
        host_name:   Optional host target name filter (substring).
        time_window: How far back to look. Examples:
                       20m, 1h, 3h, 6h, 12h, daily, last_7_days, 2d
                     Append :raw/:hourly/:daily to force rollup granularity.
                     Default: daily.

    Returns:
        JSON list of hosts sorted by avg CPU descending.  1 SQL call.
    """
    try:
        start_s, end_s, _ = _time_range(time_window)
    except ValueError as exc:
        return json.dumps({"error": str(exc)})

    name_clause = (f"AND t.target_name LIKE '%{host_name}%'"
                   if host_name else "")

    # One SQL: pivot cpu/mem/swap using conditional aggregation
    try:
        rows = _sql(f"""
            SELECT
                t.target_name,
                ROUND(AVG(CASE WHEN m.metric_column='cpuUtil'
                               THEN TO_NUMBER(m.value) END), 2) AS cpu_avg,
                ROUND(MAX(CASE WHEN m.metric_column='cpuUtil'
                               THEN TO_NUMBER(m.value) END), 2) AS cpu_max,
                ROUND(AVG(CASE WHEN m.metric_column='memUsedPct'
                               THEN TO_NUMBER(m.value) END), 2) AS mem_avg,
                ROUND(MAX(CASE WHEN m.metric_column='memUsedPct'
                               THEN TO_NUMBER(m.value) END), 2) AS mem_max,
                ROUND(AVG(CASE WHEN m.metric_column='swapUtil'
                               THEN TO_NUMBER(m.value) END), 2) AS swap_avg,
                COUNT(CASE WHEN m.metric_column='cpuUtil'
                           THEN 1 END)                          AS samples
            FROM sysman.mgmt$target t
            JOIN sysman.mgmt$metric_details m
              ON m.target_guid = t.target_guid
            WHERE t.target_type = 'host'
              AND m.metric_name = 'Load'
              AND m.metric_column IN ('cpuUtil','memUsedPct','swapUtil')
              AND m.collection_timestamp
                    BETWEEN TO_TIMESTAMP('{start_s}','YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')
                        AND TO_TIMESTAMP('{end_s}',  'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')
              {name_clause}
            GROUP BY t.target_name
            ORDER BY cpu_avg DESC NULLS LAST
        """)

        result = [{"host":      _v(r, "TARGET_NAME"),
                   "cpu_avg":   _v(r, "CPU_AVG"),
                   "cpu_max":   _v(r, "CPU_MAX"),
                   "mem_avg":   _v(r, "MEM_AVG"),
                   "mem_max":   _v(r, "MEM_MAX"),
                   "swap_avg":  _v(r, "SWAP_AVG"),
                   "samples":   _v(r, "SAMPLES")} for r in rows]

        return json.dumps({"count": len(result), "time_window": time_window,
                           "start": start_s, "end": end_s,
                           "hosts": result}, indent=2)
    except Exception as exc:
        return _err(exc)


# ═════════════════════════════════════════════════════════════════════════════
# Tool 4 — get_database_utilization
# API calls: 2 SQL  (replaces 500+ REST calls for 30 DBs)
# ═════════════════════════════════════════════════════════════════════════════
@mcp.tool()
def get_database_utilization(
    db_name:     str = "",
    time_window: str = "daily",
) -> str:
    """
    Comprehensive utilisation metrics for Oracle database targets.
    Two SQL queries cover all metrics — no per-database REST loops.

    Metrics returned:
      CPU    : host_cpu_pct, instance_cpu_pct, db_cpu_time_pct
      Memory : sga_mb, pga_mb, buffer_cache_mb, shared_pool_mb
      I/O    : io_mb_per_sec, io_requests_per_sec
      Sessions: active_sessions_cpu, sessions_io_wait, session_limit_pct
      Efficiency: library_cache_hit_pct, response_time_per_txn
      Throughput: commits_per_sec, rollbacks_per_sec

    Args:
        db_name:     Optional database target name filter (substring).
                     Works for oracle_database and rac_database.
        time_window: How far back to look. Examples:
                       20m, 1h, 3h, 6h, 12h, daily, last_7_days, 2d
                     Append :raw/:hourly/:daily to override rollup.
                     Default: daily.

    Returns:
        JSON list of databases sorted by host CPU descending.  2 SQL calls.
    """
    try:
        start_s, end_s, _ = _time_range(time_window)
    except ValueError as exc:
        return json.dumps({"error": str(exc)})

    name_clause = (f"AND t.target_name LIKE '%{db_name}%'"
                   if db_name else "")
    type_clause = ("AND t.target_type IN ('oracle_database','rac_database')")

    try:
        # ── Query 1: time-series metrics via mgmt$metric_details ──────────
        # Covers: CPU, sessions, I/O, efficiency, throughput
        # All metrics pivoted in one query using conditional aggregation
        hist = _sql(f"""
            SELECT
                t.target_name,
                t.target_type,
                -- CPU
                ROUND(AVG(CASE WHEN m.metric_name='wait_bottlenecks'
                               AND m.metric_column='host_cpu_usage_pct'
                               THEN TO_NUMBER(m.value) END), 2) AS host_cpu_avg,
                ROUND(MAX(CASE WHEN m.metric_name='wait_bottlenecks'
                               AND m.metric_column='host_cpu_usage_pct'
                               THEN TO_NUMBER(m.value) END), 2) AS host_cpu_max,
                ROUND(AVG(CASE WHEN m.metric_name='wait_bottlenecks'
                               AND m.metric_column='avg_user_cpu_time_pct'
                               THEN TO_NUMBER(m.value) END), 2) AS inst_cpu_avg,
                ROUND(AVG(CASE WHEN m.metric_name='instance_efficiency'
                               AND m.metric_column='cpu_time_pct'
                               THEN TO_NUMBER(m.value) END), 2) AS db_cpu_pct_avg,
                -- Sessions
                ROUND(AVG(CASE WHEN m.metric_name='wait_bottlenecks'
                               AND m.metric_column='user_cpu_time_cnt'
                               THEN TO_NUMBER(m.value) END), 1) AS sessions_cpu_avg,
                ROUND(AVG(CASE WHEN m.metric_name='wait_bottlenecks'
                               AND m.metric_column='userio_wait_cnt'
                               THEN TO_NUMBER(m.value) END), 1) AS sessions_io_avg,
                ROUND(AVG(CASE WHEN m.metric_name='wait_bottlenecks'
                               AND m.metric_column='user_wait_time_pct'
                               THEN TO_NUMBER(m.value) END), 2) AS wait_time_pct_avg,
                -- I/O
                ROUND(AVG(CASE WHEN m.metric_name='instance_throughput'
                               AND m.metric_column='iombs_ps'
                               THEN TO_NUMBER(m.value) END), 2) AS io_mb_ps_avg,
                ROUND(MAX(CASE WHEN m.metric_name='instance_throughput'
                               AND m.metric_column='iombs_ps'
                               THEN TO_NUMBER(m.value) END), 2) AS io_mb_ps_max,
                ROUND(AVG(CASE WHEN m.metric_name='instance_throughput'
                               AND m.metric_column='iorequests_ps'
                               THEN TO_NUMBER(m.value) END), 1) AS io_req_ps_avg,
                -- Throughput
                ROUND(AVG(CASE WHEN m.metric_name='instance_throughput'
                               AND m.metric_column='commits_ps'
                               THEN TO_NUMBER(m.value) END), 2) AS commits_ps_avg,
                ROUND(AVG(CASE WHEN m.metric_name='instance_throughput'
                               AND m.metric_column='rollbacks_ps'
                               THEN TO_NUMBER(m.value) END), 2) AS rollbacks_ps_avg,
                -- Efficiency
                ROUND(AVG(CASE WHEN m.metric_name='instance_efficiency'
                               AND m.metric_column='libcache_hit_pct'
                               THEN TO_NUMBER(m.value) END), 2) AS lib_cache_hit_avg,
                ROUND(AVG(CASE WHEN m.metric_name='instance_efficiency'
                               AND m.metric_column='response_time_pt'
                               THEN TO_NUMBER(m.value) END), 4) AS resp_time_txn_avg,
                -- Resource limits
                ROUND(AVG(CASE WHEN m.metric_name='Database_Resource_Usage'
                               AND m.metric_column='session_usage'
                               THEN TO_NUMBER(m.value) END), 1) AS session_limit_avg,
                ROUND(MAX(CASE WHEN m.metric_name='Database_Resource_Usage'
                               AND m.metric_column='session_usage'
                               THEN TO_NUMBER(m.value) END), 1) AS session_limit_max,
                ROUND(AVG(CASE WHEN m.metric_name='Database_Resource_Usage'
                               AND m.metric_column='process_usage'
                               THEN TO_NUMBER(m.value) END), 1) AS process_limit_avg,
                COUNT(CASE WHEN m.metric_name='wait_bottlenecks'
                           AND m.metric_column='host_cpu_usage_pct'
                           THEN 1 END)                           AS samples
            FROM sysman.mgmt$target t
            JOIN sysman.mgmt$metric_details m
              ON m.target_guid = t.target_guid
            WHERE {type_clause[4:]}
              AND m.metric_name IN (
                    'wait_bottlenecks','instance_throughput',
                    'instance_efficiency','Database_Resource_Usage'
                  )
              AND m.collection_timestamp
                    BETWEEN TO_TIMESTAMP('{start_s}','YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')
                        AND TO_TIMESTAMP('{end_s}',  'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')
              {name_clause}
            GROUP BY t.target_name, t.target_type
            ORDER BY host_cpu_avg DESC NULLS LAST
        """)

        # ── Query 2: current SGA/PGA snapshot from mgmt$metric_current ───
        # Memory values don't benefit from time-series avg — latest is enough
        mem = _sql(f"""
            SELECT
                t.target_name,
                ROUND(MAX(CASE WHEN m.metric_column='sga_total'
                               THEN TO_NUMBER(m.value) END), 1) AS sga_mb,
                ROUND(MAX(CASE WHEN m.metric_column='pga_total'
                               THEN TO_NUMBER(m.value) END), 1) AS pga_mb,
                ROUND(MAX(CASE WHEN m.metric_column='buffer_cache'
                               THEN TO_NUMBER(m.value) END), 1) AS buffer_cache_mb,
                ROUND(MAX(CASE WHEN m.metric_column='shared_pool'
                               THEN TO_NUMBER(m.value) END), 1) AS shared_pool_mb
            FROM sysman.mgmt$target t
            JOIN sysman.mgmt$metric_current m
              ON m.target_guid = t.target_guid
            WHERE {type_clause[4:]}
              AND m.metric_name = 'memory_usage_sga_pga'
              AND m.metric_column IN (
                    'sga_total','pga_total','buffer_cache','shared_pool'
                  )
              {name_clause}
            GROUP BY t.target_name
        """)

        # Index memory results by target name for O(1) join
        mem_idx = {_v(r, "TARGET_NAME"): r for r in mem}

        # Merge
        result = []
        for r in hist:
            name = _v(r, "TARGET_NAME")
            m    = mem_idx.get(name, {})
            result.append({
                "target":      name,
                "type":        _v(r, "TARGET_TYPE"),
                "time_window": time_window,
                "samples":     _v(r, "SAMPLES"),
                "cpu": {
                    "host_cpu_avg":    _v(r, "HOST_CPU_AVG"),
                    "host_cpu_max":    _v(r, "HOST_CPU_MAX"),
                    "instance_cpu_avg":_v(r, "INST_CPU_AVG"),
                    "db_cpu_pct_avg":  _v(r, "DB_CPU_PCT_AVG"),
                },
                "memory": {
                    "sga_mb":          _v(m, "SGA_MB"),
                    "pga_mb":          _v(m, "PGA_MB"),
                    "buffer_cache_mb": _v(m, "BUFFER_CACHE_MB"),
                    "shared_pool_mb":  _v(m, "SHARED_POOL_MB"),
                },
                "sessions": {
                    "active_cpu_avg":  _v(r, "SESSIONS_CPU_AVG"),
                    "io_wait_avg":     _v(r, "SESSIONS_IO_AVG"),
                    "wait_time_pct":   _v(r, "WAIT_TIME_PCT_AVG"),
                    "session_limit_avg": _v(r, "SESSION_LIMIT_AVG"),
                    "session_limit_max": _v(r, "SESSION_LIMIT_MAX"),
                    "process_limit_avg": _v(r, "PROCESS_LIMIT_AVG"),
                },
                "io": {
                    "io_mb_ps_avg":  _v(r, "IO_MB_PS_AVG"),
                    "io_mb_ps_max":  _v(r, "IO_MB_PS_MAX"),
                    "io_req_ps_avg": _v(r, "IO_REQ_PS_AVG"),
                },
                "throughput": {
                    "commits_ps_avg":   _v(r, "COMMITS_PS_AVG"),
                    "rollbacks_ps_avg": _v(r, "ROLLBACKS_PS_AVG"),
                },
                "efficiency": {
                    "lib_cache_hit_pct":  _v(r, "LIB_CACHE_HIT_AVG"),
                    "response_time_per_txn": _v(r, "RESP_TIME_TXN_AVG"),
                },
            })

        return json.dumps({"count": len(result), "start": start_s,
                           "end": end_s, "databases": result}, indent=2)
    except Exception as exc:
        return _err(exc)


# ═════════════════════════════════════════════════════════════════════════════
# Tool 5 — get_database_top_wait_events
# API calls: 1 SQL
# ═════════════════════════════════════════════════════════════════════════════
@mcp.tool()
def get_database_top_wait_events(
    db_name:     str = "",
    time_window: str = "daily",
    top_n:       int = 10,
) -> str:
    """
    Get top N wait events for Oracle database targets from EM metric history.
    Single SQL query — no per-database REST loops.

    Confirmed metric group from EM 24ai (oracle_database + rac_database):
      topWaitEvents group metrics:
        waitEventName      -> Wait event name (key column)
        averageWaitTime    -> Average wait time (ms)
        totalWaits         -> Total number of waits
        totalWaitTime      -> Total wait time (seconds)
        averageWaitTimeFG  -> Average foreground wait time (ms)
        totalWaitsFG       -> Total foreground waits
        totalWaitTimeFG    -> Total foreground wait time (seconds)
        waitClassName      -> Wait class name

    Args:
        db_name:     Optional database target name filter (substring).
                     Matches oracle_database and rac_database targets.
                     Leave blank for all databases.
        time_window: How far back to look. Examples:
                       20m, 1h, 3h, 6h, 12h, daily, last_7_days, 2d
                     Append :raw/:hourly/:daily to override rollup granularity.
                     Default: daily.
        top_n:       Number of top wait events per database ranked by average
                     wait time. Default: 10.

    Returns:
        JSON grouped by database, each with top N wait events sorted by
        avg wait time descending. 1 SQL call total.
    """
    try:
        start_s, end_s, _ = _time_range(time_window)
    except ValueError as exc:
        return json.dumps({"error": str(exc)})

    name_clause = f"AND t.target_name LIKE '%{db_name}%'" if db_name else ""

    try:
        rows = _sql(f"""
            SELECT *
            FROM (
                SELECT
                    t.target_name,
                    t.target_type,
                    m.key_value                                          AS wait_event,
                    ROUND(AVG(CASE WHEN m.metric_column = 'averageWaitTime'
                                   THEN TO_NUMBER(m.value) END), 4)     AS avg_wait_ms,
                    ROUND(MAX(CASE WHEN m.metric_column = 'averageWaitTime'
                                   THEN TO_NUMBER(m.value) END), 4)     AS max_wait_ms,
                    ROUND(AVG(CASE WHEN m.metric_column = 'averageWaitTimeFG'
                                   THEN TO_NUMBER(m.value) END), 4)     AS avg_fg_wait_ms,
                    ROUND(SUM(CASE WHEN m.metric_column = 'totalWaits'
                                   THEN TO_NUMBER(m.value) END), 0)     AS total_waits,
                    ROUND(SUM(CASE WHEN m.metric_column = 'totalWaitsFG'
                                   THEN TO_NUMBER(m.value) END), 0)     AS total_fg_waits,
                    ROUND(SUM(CASE WHEN m.metric_column = 'totalWaitTime'
                                   THEN TO_NUMBER(m.value) END), 2)     AS total_wait_sec,
                    MAX(CASE WHEN m.metric_column = 'waitClassName'
                             THEN m.value END)                           AS wait_class,
                    COUNT(CASE WHEN m.metric_column = 'averageWaitTime'
                               THEN 1 END)                               AS samples,
                    RANK() OVER (
                        PARTITION BY t.target_name
                        ORDER BY AVG(CASE WHEN m.metric_column = 'averageWaitTime'
                                         THEN TO_NUMBER(m.value) END) DESC NULLS LAST
                    )                                                    AS rnk
                FROM sysman.mgmt$target t
                JOIN sysman.mgmt$metric_details m
                  ON m.target_guid = t.target_guid
                WHERE t.target_type IN ('oracle_database', 'rac_database')
                  AND m.metric_name  = 'topWaitEvents'
                  AND m.metric_column IN (
                        'averageWaitTime', 'averageWaitTimeFG',
                        'totalWaits', 'totalWaitsFG',
                        'totalWaitTime', 'waitClassName'
                      )
                  AND m.collection_timestamp
                        BETWEEN TO_TIMESTAMP(
                                    '{start_s}',
                                    'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')
                            AND TO_TIMESTAMP(
                                    '{end_s}',
                                    'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')
                  {name_clause}
                GROUP BY t.target_name, t.target_type, m.key_value
            )
            WHERE rnk <= {top_n}
            ORDER BY target_name, rnk
        """, max_rows=1000)

        if not rows:
            return json.dumps({
                "count":       0,
                "time_window": time_window,
                "start":       start_s,
                "end":         end_s,
                "message":     "No wait event data found. "
                               "Try a wider time_window or check db_name filter.",
                "databases":   [],
            }, indent=2)

        # Group by database
        grouped: dict = {}
        for r in rows:
            db = _v(r, "TARGET_NAME")
            if db not in grouped:
                grouped[db] = {
                    "target":      db,
                    "type":        _v(r, "TARGET_TYPE"),
                    "wait_events": [],
                }
            grouped[db]["wait_events"].append({
                "rank":          _v(r, "RNK"),
                "wait_event":    _v(r, "WAIT_EVENT"),
                "wait_class":    _v(r, "WAIT_CLASS"),
                "avg_wait_ms":   _v(r, "AVG_WAIT_MS"),
                "max_wait_ms":   _v(r, "MAX_WAIT_MS"),
                "avg_fg_wait_ms":_v(r, "AVG_FG_WAIT_MS"),
                "total_waits":   _v(r, "TOTAL_WAITS"),
                "total_fg_waits":_v(r, "TOTAL_FG_WAITS"),
                "total_wait_sec":_v(r, "TOTAL_WAIT_SEC"),
                "samples":       _v(r, "SAMPLES"),
            })

        return json.dumps({
            "count":       len(grouped),
            "time_window": time_window,
            "start":       start_s,
            "end":         end_s,
            "top_n":       top_n,
            "databases":   list(grouped.values()),
        }, indent=2)

    except Exception as exc:
        return _err(exc)


# ═════════════════════════════════════════════════════════════════════════════
# Tool — get_host_details
# API calls: 2 SQL
# All field names confirmed from actual EM 24ai data
# ═════════════════════════════════════════════════════════════════════════════
@mcp.tool()
def get_host_details(host_name: str = "") -> str:
    """
    Get comprehensive details for host targets covering 10 key areas:
      1.  OS name, version & distribution
      2.  OS kernel / patch level
      3.  Platform & hardware type (e.g. InfinibandHost)
      4.  Virtual server type (e.g. DOMU/Xen)
      5.  IP address & FQDN
      6.  Last boot time
      7.  EM Agent version, RU patch & home directory
      8.  Current CPU utilisation % (cpuUtil, cpuUser, cpuKernel, cpuIOWait)
      9.  Current memory & swap % (memUsedPct, swapUtil, noOfProcs)
      10. Filesystem usage per mount point
          (size MB, available MB, pctAvailable, device — all confirmed)

    Confirmed metric/property names from EM 24ai:
      mgmt$target_properties (host)  : OS, Version, Distribution, OS_patchlevel,
                                        Platform, HARDWARE_TYPE, IP_address,
                                        Boottime, os.hostname, os.virtual.server,
                                        orcl_gtp_line_of_bus
      mgmt$target_properties (agent) : Version, RUVersion, AgentOracleHome,
                                        AgentBaseDir
      mgmt$metric_current Load       : cpuUtil, cpuUser, cpuKernel, cpuIOWait,
                                        cpuSteal, memUsedPct, swapUtil,
                                        noOfProcs, noOfUsers
      mgmt$metric_current Filesystems: size, available, pctAvailable, fileSystem
                                        (key_value = mount point)

    Args:
        host_name: Optional host target name filter (substring).
                   Leave blank for all hosts.

    Returns:
        JSON list of hosts with all detail areas. 2 SQL calls total.
    """
    name_clause    = f"AND t.target_name LIKE '%{host_name}%'" if host_name else ""
    name_clause_hn = f"AND t.host_name  LIKE '%{host_name}%'" if host_name else ""

    try:
        # ── Query 1: static host properties ──────────────────────────────
        props = _sql(f"""
            SELECT
                t.target_name,
                t.host_name,
                avail.availability_status                                   AS status,
                MAX(CASE WHEN p.property_name = 'OS'
                         THEN p.property_value END)                         AS os_name,
                MAX(CASE WHEN p.property_name = 'Version'
                         THEN p.property_value END)                         AS os_version,
                MAX(CASE WHEN p.property_name = 'Distribution'
                         THEN p.property_value END)                         AS distribution,
                MAX(CASE WHEN p.property_name = 'OS_patchlevel'
                         THEN p.property_value END)                         AS os_kernel,
                MAX(CASE WHEN p.property_name = 'os.release'
                         THEN p.property_value END)                         AS os_release,
                MAX(CASE WHEN p.property_name = 'Platform'
                         THEN p.property_value END)                         AS platform,
                MAX(CASE WHEN p.property_name = 'HARDWARE_TYPE'
                         THEN p.property_value END)                         AS hardware_type,
                MAX(CASE WHEN p.property_name = 'IP_address'
                         THEN p.property_value END)                         AS ip_address,
                MAX(CASE WHEN p.property_name = 'Boottime'
                         THEN p.property_value END)                         AS boot_time,
                MAX(CASE WHEN p.property_name = 'os.hostname'
                         THEN p.property_value END)                         AS fqdn,
                MAX(CASE WHEN p.property_name = 'os.virtual.server'
                         THEN p.property_value END)                         AS virtual_type,
                MAX(CASE WHEN p.property_name = 'orcl_gtp_line_of_bus'
                         THEN p.property_value END)                         AS line_of_business
            FROM sysman.mgmt$target t
            LEFT JOIN sysman.mgmt$target_properties p
                   ON p.target_guid = t.target_guid
            LEFT JOIN sysman.mgmt$availability_current avail
                   ON avail.target_guid = t.target_guid
            WHERE t.target_type = 'host'
              {name_clause}
            GROUP BY t.target_name, t.host_name, avail.availability_status
            ORDER BY t.target_name
        """)

        if not props:
            return json.dumps({"error": "No host targets found", "count": 0})

        # Agent version from oracle_emd target joined by host_name
        agents = _sql(f"""
            SELECT
                t.host_name,
                MAX(CASE WHEN p.property_name = 'Version'
                         THEN p.property_value END)                         AS agent_version,
                MAX(CASE WHEN p.property_name = 'RUVersion'
                         THEN p.property_value END)                         AS agent_ru,
                MAX(CASE WHEN p.property_name = 'AgentOracleHome'
                         THEN p.property_value END)                         AS agent_home,
                MAX(CASE WHEN p.property_name = 'AgentBaseDir'
                         THEN p.property_value END)                         AS agent_base,
                avail.availability_status                                   AS agent_status
            FROM sysman.mgmt$target t
            LEFT JOIN sysman.mgmt$target_properties p
                   ON p.target_guid = t.target_guid
            LEFT JOIN sysman.mgmt$availability_current avail
                   ON avail.target_guid = t.target_guid
            WHERE t.target_type = 'oracle_emd'
              {name_clause_hn}
            GROUP BY t.host_name, avail.availability_status
        """)
        agent_idx = {_v(a, "HOST_NAME"): a for a in agents}

        # ── Query 2: live metrics (Load + Filesystems) ────────────────────
        metrics = _sql(f"""
            SELECT
                t.target_name,
                m.metric_name,
                m.metric_column,
                m.key_value,
                m.value,
                TO_CHAR(m.collection_timestamp,'YYYY-MM-DD HH24:MI:SS') AS collected_at
            FROM sysman.mgmt$target t
            JOIN sysman.mgmt$metric_current m
              ON m.target_guid = t.target_guid
            WHERE t.target_type = 'host'
              AND (
                    (m.metric_name = 'Load'
                     AND m.metric_column IN (
                           'cpuUtil','cpuUser','cpuKernel','cpuIOWait',
                           'cpuSteal','memUsedPct','swapUtil',
                           'noOfProcs','noOfUsers'
                         )
                    )
                 OR
                    (m.metric_name = 'Filesystems'
                     AND m.metric_column IN (
                           'size','available','pctAvailable','fileSystem'
                         )
                    )
                  )
              {name_clause}
            ORDER BY t.target_name, m.metric_name, m.key_value, m.metric_column
        """, max_rows=2000)

        # ── Index metrics by host ─────────────────────────────────────────
        from collections import defaultdict
        host_metrics = defaultdict(lambda: {
            "cpu_util":    None, "cpu_user":  None,
            "cpu_kernel":  None, "cpu_iowait":None,
            "cpu_steal":   None, "mem_pct":   None,
            "swap_pct":    None, "num_procs": None,
            "num_users":   None, "collected_at": None,
            "filesystems": {},
        })

        for m in metrics:
            name = _v(m, "TARGET_NAME")
            col  = _v(m, "METRIC_COLUMN")
            key  = _v(m, "KEY_VALUE") or ""
            val  = _v(m, "VALUE")
            ts   = _v(m, "COLLECTED_AT")
            hm   = host_metrics[name]

            if not hm["collected_at"] and ts:
                hm["collected_at"] = ts

            # Load metrics
            if col == "cpuUtil":      hm["cpu_util"]   = val
            elif col == "cpuUser":    hm["cpu_user"]   = val
            elif col == "cpuKernel":  hm["cpu_kernel"] = val
            elif col == "cpuIOWait":  hm["cpu_iowait"] = val
            elif col == "cpuSteal":   hm["cpu_steal"]  = val
            elif col == "memUsedPct": hm["mem_pct"]    = val
            elif col == "swapUtil":   hm["swap_pct"]   = val
            elif col == "noOfProcs":  hm["num_procs"]  = val
            elif col == "noOfUsers":  hm["num_users"]  = val

            # Filesystem metrics — keyed by mount point
            elif key:
                fs = hm["filesystems"].setdefault(key, {})
                if col == "size":
                    fs["size_mb"] = val
                elif col == "available":
                    fs["available_mb"] = val
                elif col == "pctAvailable":
                    # EM stores pctAvailable — derive used %
                    try:
                        fs["pct_available"] = float(val)
                        fs["pct_used"]      = round(100 - float(val), 2)
                    except (TypeError, ValueError):
                        fs["pct_available"] = val
                elif col == "fileSystem":
                    fs["device"] = val

        # ── Merge ─────────────────────────────────────────────────────────
        result = []
        for p in props:
            name = _v(p, "TARGET_NAME")
            hn   = _v(p, "HOST_NAME")
            hm   = host_metrics.get(name, {})
            ag   = agent_idx.get(hn, {})

            # Build filesystem list sorted by used % descending
            fs_list = sorted([
                {
                    "mount":         mount,
                    "device":        info.get("device"),
                    "size_mb":       info.get("size_mb"),
                    "available_mb":  info.get("available_mb"),
                    "pct_used":      info.get("pct_used"),
                    "pct_available": info.get("pct_available"),
                }
                for mount, info in hm.get("filesystems", {}).items()
            ], key=lambda x: float(x["pct_used"] or 0), reverse=True)

            result.append({
                "host":             name,
                "fqdn":             _v(p, "FQDN"),
                "ip_address":       _v(p, "IP_ADDRESS"),
                "status":           _v(p, "STATUS"),
                "line_of_business": _v(p, "LINE_OF_BUSINESS"),
                "boot_time":        _v(p, "BOOT_TIME"),
                "os": {
                    "name":         _v(p, "OS_NAME"),
                    "version":      _v(p, "OS_VERSION"),
                    "distribution": _v(p, "DISTRIBUTION"),
                    "kernel":       _v(p, "OS_KERNEL"),
                    "release":      _v(p, "OS_RELEASE"),
                    "platform":     _v(p, "PLATFORM"),
                    "hardware_type":_v(p, "HARDWARE_TYPE"),
                    "virtual_type": _v(p, "VIRTUAL_TYPE"),
                },
                "agent": {
                    "version":      _v(ag, "AGENT_VERSION"),
                    "ru_version":   _v(ag, "AGENT_RU"),
                    "home":         _v(ag, "AGENT_HOME"),
                    "base_dir":     _v(ag, "AGENT_BASE"),
                    "status":       _v(ag, "AGENT_STATUS"),
                },
                "cpu": {
                    "util_pct":     hm.get("cpu_util"),
                    "user_pct":     hm.get("cpu_user"),
                    "kernel_pct":   hm.get("cpu_kernel"),
                    "iowait_pct":   hm.get("cpu_iowait"),
                    "steal_pct":    hm.get("cpu_steal"),
                },
                "memory": {
                    "used_pct":     hm.get("mem_pct"),
                    "swap_pct":     hm.get("swap_pct"),
                    "num_processes":hm.get("num_procs"),
                    "num_users":    hm.get("num_users"),
                },
                "filesystems":      fs_list,
                "metrics_at":       hm.get("collected_at"),
            })

        return json.dumps({"count": len(result), "hosts": result}, indent=2)

    except Exception as exc:
        return _err(exc)



# ═════════════════════════════════════════════════════════════════════════════
# Tool — get_database_version_and_others
# API calls: 2 REST  (no SQL)
# Confirmed from actual EM 24ai REST API response
# can be retrived other info below
# ═════════════════════════════════════════════════════════════════════════════
@mcp.tool()
def get_database_version(db_name: str = "") -> str:
    """
    Get database version and key configuration for Oracle database targets.
    Pure REST API — no SQL required.

    REST endpoints used:
      GET /em/api/targets?typeName=oracle_database  -> target list
      GET /em/api/targets/{id}/properties           -> 159 properties per target

    Confirmed property id keys from EM 24ai:
      DBVersion        -> base version        e.g. 23.26.1.0.0
      VersionBanner    -> full edition banner  e.g. Oracle AI Database 26ai EE...
      VersionCategory  -> edition category
      CDB              -> YES / NO
      DBName           -> database name
      DBDomain         -> domain
      DBCategoryDetails-> category            e.g. FullLLFile+CDB
      CPUCount         -> CPU count
      DbBlockSize      -> block size in bytes
      DataGuardStatus  -> None / configured
      RACOption        -> YES / NO
      OracleHome       -> Oracle home path

    Args:
        db_name: Optional substring filter on target name (case-insensitive).
                 Matches oracle_database type targets.
                 Leave blank for all instances.

    Returns:
        JSON list sorted by target name. 1 + N REST calls
        (1 target list + 1 properties call per matched target).
    """
    try:
        # ── Step 1: list oracle_database targets ──────────────────────────
        # Confirmed field names: id, name, typeName, displayName,
        #   hostName, status, statusDisplayName
        with _client() as c:
            r = c.get("/api/targets", params={
                "typeName": "oracle_database",
                "limit":    200,
            })
            r.raise_for_status()
            all_targets = r.json().get("items", [])

        if db_name:
            targets = [
                t for t in all_targets
                if db_name.lower() in (t.get("name") or "").lower()
            ]
        else:
            targets = all_targets

        if not targets:
            return json.dumps({
                "error": f"No oracle_database targets found matching '{db_name}'",
                "count": 0,
            })

        # ── Step 2: fetch properties per target ───────────────────────────
        # Properties use 'id' field as the key (not 'name' — display names differ)
        # e.g. id='DBVersion' but name='Version', id='DBName' but name='Database Name'
        WANT = {
            "DBVersion", "VersionBanner", "VersionCategory",
            "CDB", "DBName", "DBDomain", "DBCategoryDetails",
            "CPUCount", "DbBlockSize", "DataGuardStatus",
            "RACOption", "OracleHome", "InstanceName",
            "base_db_version", "StatisticsLevel",
        }

        results = []
        with _client() as c:
            for t in targets:
                tid  = t.get("id")
                tname = t.get("name")

                if not tid:
                    results.append({"target": tname, "error": "no id"})
                    continue

                try:
                    pr = c.get(f"/api/targets/{tid}/properties",
                               params={"limit": 2000})
                    pr.raise_for_status()

                    # Build dict keyed by property 'id' (not 'name')
                    props = {
                        item["id"]: item.get("value")
                        for item in pr.json().get("items", [])
                        if item.get("id") in WANT
                    }

                    results.append({
                        "target":           tname,
                        "host":             t.get("hostName"),
                        "status":           t.get("statusDisplayName"),
                        "db_version":       props.get("DBVersion"),
                        "version_banner":   props.get("VersionBanner"),
                        "version_category": props.get("VersionCategory"),
                        "db_category":      props.get("DBCategoryDetails"),
                        "is_cdb":           props.get("CDB"),
                        "is_rac":           props.get("RACOption"),
                        "db_name":          props.get("DBName"),
                        "db_domain":        props.get("DBDomain"),
                        "instance_name":    props.get("InstanceName"),
                        "cpu_count":        props.get("CPUCount"),
                        "block_size_bytes": props.get("DbBlockSize"),
                        "oracle_home":      props.get("OracleHome"),
                        "dg_status":        props.get("DataGuardStatus"),
                    })

                except httpx.HTTPStatusError as exc:
                    results.append({
                        "target": tname,
                        "error":  f"API {exc.response.status_code}: "
                                  f"{exc.response.text[:150]}",
                    })
                except Exception as exc:
                    results.append({"target": tname, "error": str(exc)})

        results.sort(key=lambda x: x.get("target") or "")

        return json.dumps({
            "count":     len(results),
            "databases": results,
        }, indent=2)

    except Exception as exc:
        return _api_err(exc)



# ═════════════════════════════════════════════════════════════════════════════
# Tool — get_open_incidents
# Uses: GET /em/api/incidents
# Ref:  https://docs.oracle.com/en/enterprise-manager/cloud-control/
#       enterprise-manager-cloud-control/24.1/emrest/
#       op-https-em_host-em_console_https_port-em-api-incidents-get.html
# ═════════════════════════════════════════════════════════════════════════════
@mcp.tool()
def get_open_incidents(
    severity:        str = "",
    target_type:     str = "",
    max_results:     int = 10,
    age_hours:       int = 744,
) -> str:
    """
    List open incidents from Oracle EM using the REST API.

    Endpoint: GET /em/api/incidents
    Source: EM 24.1 REST API documentation

    Confirmed query parameters:
      isOpen=true                      — open incidents only
      severity                         — FATAL|CRITICAL|WARNING|MINOR_WARNING|CLEAR
      targetTypeName                   — EM internal target type name
      sort=timeCreated:DESC            — newest first
      ageInHoursLessThanOrEqualTo      — default 744 (31 days)
      limit                            — page size
      page                             — pagination token from next link

    NOTE: targetName is NOT used — it requires an exact full match and
    causes HTTP 500 for partial names. Filter by target_type instead,
    or leave blank to get all open incidents.

    Confirmed response fields (IncidentSummary schema):
      displayId, message, severity, status, isOpen,
      isAcknowledged, isEscalated, ageInHours,
      timeCreated, timeUpdated,
      targets[].name, targets[].typeName, targets[].typeDisplayName,
      priority.displayName, escalationLevel.displayName

    Args:
        severity:    Optional severity filter.
                     Accepted values: fatal, critical, warning,
                     minor_warning, clear  (case-insensitive).
        target_type: Optional target type filter. Accepted aliases:
                       host, database, pdb, rac, zdlra, exadata,
                       exadata_cloud, vm_cluster, exadb, dbsys etc.
                     Resolved to EM internal type via _resolve_type().
        max_results: Max incidents to return. Default 10, capped at 10.
        age_hours:   Only return incidents newer than this many hours.
                     Default 744 (31 days). Use 0 for no limit.

    Returns:
        JSON list of open incidents sorted by most recently created first.
    """
    limit = min(max(1, max_results), 10)

    # Severity values confirmed from EM 24.1 docs (must be uppercase)
    SEV_MAP = {
        "fatal":         "FATAL",
        "critical":      "CRITICAL",
        "warning":       "WARNING",
        "minor_warning": "MINOR_WARNING",
        "advisory":      "MINOR_WARNING",
        "clear":         "CLEAR",
    }

    # Build request params — only use confirmed supported params
    params: dict = {
        "isOpen": "true",
        "limit":  100,          # fetch enough to fill limit after pagination
        "sort":   "timeCreated:DESC",
    }

    if severity:
        sev = severity.strip().lower()
        if sev in SEV_MAP:
            params["severity"] = SEV_MAP[sev]

    if target_type:
        params["targetTypeName"] = _resolve_type(target_type)

    if age_hours and age_hours > 0:
        params["ageInHoursLessThanOrEqualTo"] = age_hours

    # Paginate through all results
    try:
        all_items = []
        next_href = None
        page      = 0

        with _client() as c:
            while page < 20:               # safety cap
                if next_href:
                    r = c.get(next_href)   # use next link as-is
                else:
                    r = c.get("/api/incidents", params=params)

                r.raise_for_status()
                data  = r.json()
                items = data.get("items", [])
                all_items.extend(items)
                page += 1

                # Follow next link for more pages
                next_href = (
                    (data.get("links") or {})
                    .get("next", {})
                    .get("href")
                )
                if not next_href:
                    break

        # Apply final limit
        all_items = all_items[:limit]

        # Format response
        result = []
        for i in all_items:
            targets = i.get("targets") or []
            result.append({
                "incident_id":  i.get("displayId"),
                "severity":     i.get("severity"),
                "status":       i.get("status"),
                "acknowledged": i.get("isAcknowledged"),
                "escalated":    i.get("isEscalated"),
                "priority":     (i.get("priority") or {}).get("displayName"),
                "age_hours":    i.get("ageInHours"),
                "created":      i.get("timeCreated"),
                "updated":      i.get("timeUpdated"),
                "message":      i.get("message"),
                "targets": [
                    {
                        "name":         t.get("name"),
                        "type":         t.get("typeName"),
                        "type_display": t.get("typeDisplayName"),
                    }
                    for t in targets
                ],
            })

        return json.dumps({
            "count":   len(result),
            "filters": {
                "severity":    severity    or None,
                "target_type": target_type or None,
                "age_hours":   age_hours,
                "max_results": limit,
            },
            "incidents": result,
        }, indent=2)

    except Exception as exc:
        return _api_err(exc)


# ═════════════════════════════════════════════════════════════════════════════
# Tool — get_zdlra_protected_databases
# Primary source: EM REST endpoint /em/api/zdlra/{raTargetId}/protectedDatabases
# Fallback: SYSMAN metric data if the endpoint is unavailable
# ═════════════════════════════════════════════════════════════════════════════
@mcp.tool()
def get_zdlra_protected_databases(
    zdlra_name: str = "",
    db_filter:  str = "",
    ra_target_id: str = "",
    limit: int = 2000,
    page: str = "",
    name_only: bool = False,
) -> str:
    """
    List protected databases on a ZDLRA using the documented EM REST endpoint.

    Primary source:
      GET /em/api/zdlra/{raTargetId}/protectedDatabases

    Query parameters supported by EM:
      limit     - maximum rows to return (1..2000)
      nameOnly  - return only protected database names
      page      - continuation token from the previous response

    Header supported by EM:
      ORCL-EM-ZDLRATARGET-AUTHORIZATION
      Set via EM_ZDLRA_AUTHORIZATION if you need NAMED:... or PREFERRED:...

    Behavior:
      1. Resolve raTargetId from EM repository metadata if not provided.
      2. Call the ZDLRA REST endpoint and follow nextPage links.
      3. Fall back to repository metrics if the REST endpoint is unavailable.

    Args:
        zdlra_name: Optional ZDLRA target-name substring used to resolve raTargetId.
        db_filter: Optional protected database substring filter.
        ra_target_id: Optional Recovery Appliance target ID (GUID/hex).
        limit: Page size for the REST call. Clamped to 1..2000.
        page: Continuation token from a previous REST response.
        name_only: If true, request only database names from EM.

    Returns:
        JSON with items, pagination metadata, and the source used.
    """
    limit = max(1, min(int(limit or 2000), 2000))
    headers = {}
    if EM_ZDLRA_AUTHORIZATION:
        headers["ORCL-EM-ZDLRATARGET-AUTHORIZATION"] = EM_ZDLRA_AUTHORIZATION

    resolved_id = ra_target_id.strip() if ra_target_id else EM_ZDLRA_TARGET_ID
    resolved_name = zdlra_name.strip() if zdlra_name else ""

    try:
        if not resolved_id:
            resolved_id, resolved_name = _resolve_zdlra_target_id(zdlra_name)
            if not resolved_id:
                return json.dumps({
                    "error": "Unable to resolve a ZDLRA target id from EM repository metadata.",
                    "hint": "Pass ra_target_id explicitly or provide zdlra_name that matches a monitored oracle_dblra target.",
                    "filters": {
                        "zdlra_name": zdlra_name or None,
                        "db_filter": db_filter or None,
                    },
                }, indent=2)

        params = {"limit": limit}
        if name_only:
            params["nameOnly"] = "true"
        if page:
            params["page"] = page

        items: list[dict] = []
        current_page = page or None
        next_page_token = None
        first_response = None

        while True:
            request_params = {"limit": limit}
            if name_only:
                request_params["nameOnly"] = "true"
            if current_page:
                request_params["page"] = current_page

            payload = _em_request_json(
                "GET",
                f"/api/zdlra/{resolved_id}/protectedDatabases",
                params=request_params,
                headers=headers or None,
            )
            if first_response is None:
                first_response = payload
            batch = payload.get("items") or []
            if db_filter:
                batch = [
                    item for item in batch
                    if db_filter.lower() in str(
                        item.get("dbUniqueName") or item.get("targetName") or item.get("name") or ""
                    ).lower()
                ]
            items.extend(batch)
            next_page_obj = ((payload.get("links") or {}).get("nextPage")
                             or payload.get("nextPage"))
            next_page_token = _extract_page_token(next_page_obj)
            if not next_page_token:
                break
            current_page = next_page_token

        return json.dumps({
            "source": "em_rest_api",
            "ra_target_id": resolved_id,
            "zdlra_name": resolved_name,
            "count": len(items),
            "requested_limit": limit,
            "page": page or None,
            "next_page": next_page_token,
            "filters": {
                "zdlra_name": zdlra_name or None,
                "db_filter": db_filter or None,
            },
            "items": items,
            "raw": first_response,
        }, indent=2, default=str)

    except Exception as rest_exc:
        logger.warning("ZDLRA REST endpoint failed; falling back to repository metrics: %s", rest_exc)
        zdlra_clause = (f"AND UPPER(t.target_name) LIKE UPPER('%{zdlra_name}%')"
                        if zdlra_name else "")
        db_clause = (f"AND UPPER(m.key_value) LIKE UPPER('%{db_filter}%')"
                     if db_filter else "")
        try:
            rows = _sql(f"""
                SELECT
                    t.target_name                                           AS zdlra_name,
                    avail.availability_status                               AS zdlra_status,
                    m.key_value                                             AS db_unique_name,
                    MAX(CASE WHEN m.metric_column = 'last_backup_time'
                             THEN m.value END)                              AS last_backup_time,
                    MAX(CASE WHEN m.metric_column = 'last_backup_age'
                             THEN ROUND(TO_NUMBER(m.value) / 3600, 2)
                             END)                                           AS last_backup_age_hrs,
                    MAX(CASE WHEN m.metric_column = 'current_recovery_window_str'
                             THEN m.value END)                              AS recovery_window_current,
                    MAX(CASE WHEN m.metric_column = 'recovery_window_goal_str'
                             THEN m.value END)                              AS recovery_window_goal,
                    MAX(CASE WHEN m.metric_column = 'recovery_window_compliance'
                             THEN m.value END)                              AS rw_compliance,
                    MAX(CASE WHEN m.metric_column = 'unprotected_data_window'
                             THEN ROUND(TO_NUMBER(m.value) / 3600, 2)
                             END)                                           AS unprotected_data_hrs,
                    MAX(CASE WHEN m.metric_column = 'nzdl_active'
                             THEN m.value END)                              AS nzdl_active,
                    MAX(CASE WHEN m.metric_column = 'current_space'
                             THEN ROUND(TO_NUMBER(m.value), 2)
                             END)                                           AS space_used_gb,
                    MAX(CASE WHEN m.metric_column = 'dedup_ratio'
                             THEN ROUND(TO_NUMBER(m.value), 2)
                             END)                                           AS dedup_ratio,
                    MAX(CASE WHEN m.metric_column = 'sl_name'
                             THEN m.value END)                              AS storage_location,
                    TO_CHAR(MAX(m.collection_timestamp),
                            'YYYY-MM-DD HH24:MI:SS')                       AS collected_at
                FROM sysman.mgmt$metric_current m
                JOIN sysman.mgmt$target t
                  ON t.target_guid = m.target_guid
                LEFT JOIN sysman.mgmt$availability_current avail
                  ON avail.target_guid = t.target_guid
                WHERE t.target_type   = 'oracle_dblra'
                  AND m.metric_name   = 'dblra_protected_database'
                  {zdlra_clause}
                  {db_clause}
                GROUP BY t.target_name, avail.availability_status, m.key_value
                ORDER BY t.target_name, last_backup_age_hrs DESC NULLS LAST
            """, max_rows=500)

            if not rows:
                return json.dumps({
                    "error":   "No ZDLRA protected database data found from either the EM REST endpoint or repository metrics.",
                    "count":   0,
                    "source":  "fallback_sql",
                }, indent=2)

            from collections import defaultdict
            appliances: dict = {}
            for r in rows:
                zname = _v(r, "ZDLRA_NAME")
                if zname not in appliances:
                    appliances[zname] = {
                        "zdlra":        zname,
                        "status":       _v(r, "ZDLRA_STATUS"),
                        "db_count":     0,
                        "databases":    [],
                    }
                appliances[zname]["db_count"] += 1
                appliances[zname]["databases"].append({
                    "db_unique_name":       _v(r, "DB_UNIQUE_NAME"),
                    "last_backup_time":     _v(r, "LAST_BACKUP_TIME"),
                    "last_backup_age_hrs":  _v(r, "LAST_BACKUP_AGE_HRS"),
                    "recovery_window": {
                        "current":    _v(r, "RECOVERY_WINDOW_CURRENT"),
                        "goal":       _v(r, "RECOVERY_WINDOW_GOAL"),
                        "compliance": _v(r, "RW_COMPLIANCE"),
                    },
                    "unprotected_data_hrs": _v(r, "UNPROTECTED_DATA_HRS"),
                    "nzdl_active":          _v(r, "NZDL_ACTIVE"),
                    "space_used_gb":        _v(r, "SPACE_USED_GB"),
                    "dedup_ratio":          _v(r, "DEDUP_RATIO"),
                    "storage_location":     _v(r, "STORAGE_LOCATION"),
                    "collected_at":         _v(r, "COLLECTED_AT"),
                })

            return json.dumps({
                "source": "fallback_sql",
                "zdlra_count":     len(appliances),
                "total_protected": sum(a["db_count"] for a in appliances.values()),
                "filters": {
                    "zdlra_name": zdlra_name or None,
                    "db_filter":  db_filter  or None,
                },
                "appliances": list(appliances.values()),
            }, indent=2)

        except Exception as exc:
            return _err(exc)


# ═════════════════════════════════════════════════════════════════════════════
# Tool — get_database_alert_log_errors
# API calls: 2 SQL via REST
# Confirmed views: MGMT$ALERT_HISTORY + MGMT$ALERT_CURRENT
#
# Confirmed from actual EM 24ai SYSMAN schema:
#   MGMT$ALERT_HISTORY.COLLECTION_TIMESTAMP  -> DATE type
#   MGMT$ALERT_CURRENT.COLLECTION_TIMESTAMP  -> NUMBER (epoch ms)
#   Both views have: TARGET_NAME, TARGET_TYPE, METRIC_NAME, METRIC_LABEL,
#     KEY_VALUE, ALERT_STATE, MESSAGE, VIOLATION_TYPE
#   MGMT$ALERT_HISTORY also has: ALERT_DURATION
#
# metric_name values for alert log errors (confirmed from mgmt$alert_history):
#   adrAlertLogIncidentError    -> ADR incidents (ORA-7445, access violations)
#   adrAlertLogOperationalError -> operational errors
#   adrAlertLogDataFailure      -> data / media failures
# ═════════════════════════════════════════════════════════════════════════════
@mcp.tool()
def get_database_alert_log_errors(
    db_name:      str = "",
    time_window:  str = "last_7_days",
    error_filter: str = "",
    max_results:  int = 20,
) -> str:
    """
    Retrieve alert log errors and ADR incidents from Oracle EM for database
    targets. Searches both currently active and historical alert data.

    Data sources (confirmed from EM 24ai SYSMAN schema):

      MGMT$ALERT_CURRENT  — currently active alert log errors
        COLLECTION_TIMESTAMP: NUMBER (epoch milliseconds)
        metric_name values  : adrAlertLogIncidentError
                              adrAlertLogOperationalError
                              adrAlertLogDataFailure

      MGMT$ALERT_HISTORY  — historical alert log errors
        COLLECTION_TIMESTAMP: DATE type (use SYSDATE - N for filtering)
        metric_name values  : same as above
        extra column        : ALERT_DURATION

    Confirmed columns in both views:
      TARGET_NAME, TARGET_TYPE, METRIC_NAME, METRIC_LABEL,
      KEY_VALUE  (timestamp + line number of error in alert log),
      MESSAGE    (full error description),
      ALERT_STATE (Critical / Warning),
      VIOLATION_TYPE

    Note: EM only records alert log errors when Oracle ADR raises an
    incident or threshold is crossed. Healthy databases with no ADR
    incidents will return empty results — this is expected.

    Args:
        db_name:      Optional database target name filter (substring).
                      Leave blank for all oracle_database / rac_database.
        time_window:  How far back to search history. Accepts:
                        Named  : daily, last_3_days, last_7_days, last_month
                        Hours  : 1h, 6h, 12h, 48h
                        Days   : 2d, 7d, 30d
                      Default: last_7_days.
        error_filter: Optional text filter on KEY_VALUE and MESSAGE.
                      e.g. "ORA-600", "ORA-4031", "access violation"
                      Case-insensitive. Leave blank for all errors.
        max_results:  Max results per section. Default 20.

    Returns:
        JSON with current_errors (active now) and historical_errors
        (within time_window), newest first. 2 SQL calls total.

    Sample questions:
        "Any alert log errors for emrep?"
        "Show ADR incidents for all databases in the last 30 days"
        "Any ORA-600 errors in the last month?"
        "Show critical alert log errors for se26db"
        "Any data failure alerts across all databases?"
    """
    # ── Derive lookback days from time_window (for DATE comparison) ───────
    presets = {
        "daily":       1,
        "last_3_days": 3,
        "last_7_days": 7,
        "last_month":  30,
    }
    key = time_window.strip().lower().split(":")[0]  # strip rollup suffix
    if key in presets:
        lookback_days = presets[key]
    else:
        m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*(h|d|m)", key)
        if m:
            v, u = float(m.group(1)), m.group(2)
            lookback_days = {"h": v / 24, "d": v, "m": v / 1440}[u]
        else:
            try:
                lookback_days = float(key)  # bare number treated as hours
                lookback_days = lookback_days / 24
            except ValueError:
                lookback_days = 7  # safe default

    # Also get ISO start/end for display
    try:
        start_s, end_s, _ = _time_range(time_window)
    except ValueError:
        from datetime import datetime, timedelta, timezone
        now     = datetime.now(timezone.utc)
        start_s = (now - timedelta(days=lookback_days)).strftime(
                    "%Y-%m-%dT%H:%M:%S.000Z")
        end_s   = now.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    # ── Build filter clauses ──────────────────────────────────────────────
    name_clause  = (f"AND a.target_name LIKE '%{db_name}%'"
                    if db_name else "")
    error_clause = (
        f"AND (UPPER(a.key_value) LIKE UPPER('%{error_filter}%') "
        f"OR UPPER(a.message) LIKE UPPER('%{error_filter}%'))"
        if error_filter else ""
    )

    current_errors    = []
    historical_errors = []
    current_err       = None
    historical_err    = None

    # ── Query 1: currently active errors (MGMT$ALERT_CURRENT) ────────────
    # COLLECTION_TIMESTAMP is NUMBER (epoch ms) — use NUMTODSINTERVAL
    try:
        rows = _sql(f"""
            SELECT *
            FROM (
                SELECT
                    a.target_name,
                    a.target_type,
                    a.metric_name,
                    a.metric_label,
                    a.key_value,
                    a.alert_state,
                    a.violation_type,
                    a.message,
                    TO_CHAR(
                        TIMESTAMP '1970-01-01 00:00:00'
                        + NUMTODSINTERVAL(a.collection_timestamp / 1000,
                                         'SECOND'),
                        'YYYY-MM-DD HH24:MI:SS'
                    )                                       AS collected_at
                FROM sysman.mgmt$alert_current a
                WHERE a.target_type IN ('oracle_database', 'rac_database')
                  AND a.metric_name IN (
                        'adrAlertLogIncidentError',
                        'adrAlertLogOperationalError',
                        'adrAlertLogDataFailure'
                      )
                  {name_clause}
                  {error_clause}
                ORDER BY a.collection_timestamp DESC
            )
            WHERE rownum <= {max_results}
        """, max_rows=max_results)

        for r in rows:
            current_errors.append({
                "target":         _v(r, "TARGET_NAME"),
                "target_type":    _v(r, "TARGET_TYPE"),
                "error_type":     _v(r, "METRIC_LABEL"),
                "metric_name":    _v(r, "METRIC_NAME"),
                "key_value":      _v(r, "KEY_VALUE"),
                "alert_state":    _v(r, "ALERT_STATE"),
                "violation_type": _v(r, "VIOLATION_TYPE"),
                "message":        _v(r, "MESSAGE"),
                "collected_at":   _v(r, "COLLECTED_AT"),
            })
    except Exception as exc:
        current_err = str(exc)

    # ── Query 2: historical errors (MGMT$ALERT_HISTORY) ──────────────────
    # COLLECTION_TIMESTAMP is DATE type — use SYSDATE - N for filtering
    try:
        rows = _sql(f"""
            SELECT *
            FROM (
                SELECT
                    a.target_name,
                    a.target_type,
                    a.metric_name,
                    a.metric_label,
                    a.key_value,
                    a.alert_state,
                    a.alert_duration,
                    a.violation_type,
                    a.message,
                    TO_CHAR(a.collection_timestamp,
                            'YYYY-MM-DD HH24:MI:SS')       AS collected_at
                FROM sysman.mgmt$alert_history a
                WHERE a.target_type IN ('oracle_database', 'rac_database')
                  AND a.metric_name IN (
                        'adrAlertLogIncidentError',
                        'adrAlertLogOperationalError',
                        'adrAlertLogDataFailure'
                      )
                  AND a.collection_timestamp >= SYSDATE - {lookback_days}
                  {name_clause}
                  {error_clause}
                ORDER BY a.collection_timestamp DESC
            )
            WHERE rownum <= {max_results}
        """, max_rows=max_results)

        for r in rows:
            historical_errors.append({
                "target":         _v(r, "TARGET_NAME"),
                "target_type":    _v(r, "TARGET_TYPE"),
                "error_type":     _v(r, "METRIC_LABEL"),
                "metric_name":    _v(r, "METRIC_NAME"),
                "key_value":      _v(r, "KEY_VALUE"),
                "alert_state":    _v(r, "ALERT_STATE"),
                "alert_duration": _v(r, "ALERT_DURATION"),
                "violation_type": _v(r, "VIOLATION_TYPE"),
                "message":        _v(r, "MESSAGE"),
                "collected_at":   _v(r, "COLLECTED_AT"),
            })
    except Exception as exc:
        historical_err = str(exc)

    return json.dumps({
        "filters": {
            "db_name":      db_name      or None,
            "time_window":  time_window,
            "error_filter": error_filter or None,
            "start":        start_s,
            "end":          end_s,
        },
        "current_errors": {
            "count": len(current_errors),
            "error": current_err,
            "note":  "Currently active alert log errors (not yet cleared)",
            "items": current_errors,
        },
        "historical_errors": {
            "count": len(historical_errors),
            "error": historical_err,
            "note":  (f"Alert log errors in the past "
                      f"{lookback_days:.1f} day(s) — "
                      f"empty means no ADR incidents in this period"),
            "items": historical_errors,
        },
    }, indent=2)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    _validate_config()
    transport = sys.argv[1] if len(sys.argv) > 1 else MCP_TRANSPORT
    logger.info("Starting Oracle EM MCP Server  transport=%s", transport)
    mcp.run(transport="streamable-http" if transport in ("sse", "streamable-http")
            else "stdio")


