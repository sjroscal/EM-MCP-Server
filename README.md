# Oracle Enterprise Manager MCP Server

An MCP (Model Context Protocol) server that exposes Oracle Enterprise Manager
monitoring data to AI assistants like AI Client via natural language queries.

## Architecture
AI Client / Private Agent Factory <br>
│<br>
│  MCP protocol (SSE / streamable-http)<br>
▼<br>
em_mcp_server_restapi.py<br>
│<br>
└── EM REST API  (GET /em/api/*)<br>
└── EM SQL API   (POST /em/websvcs/restful/emws/.../executesql/...)<br>
│<br>
▼<br>
Oracle Enterprise Manager 24ai<br>
│<br>
▼<br>
Oracle SYSMAN Repository<br>

## Tools

| Tool | Description | API calls |
|---|---|---|
| `ping_em_repository` | EM connectivity check — version, target counts | 1 SQL |
| `list_targets` | List targets by type / name filter with status | 1 SQL |
| `get_host_cpu_utilization` | Host CPU, memory, swap % over time window | 1 SQL |
| `get_host_details` | OS, kernel, IP, agent version, filesystem usage | 2 SQL |
| `get_database_version` | DB version, edition, CDB/RAC flags | 1+N REST |
| `get_database_details` | SGA/PGA, tablespaces, init params, backup | 3 SQL |
| `get_database_utilization` | CPU, memory, I/O, sessions, throughput | 2 SQL |
| `get_database_top_wait_events` | Top N wait events per database | 1 SQL |
| `get_open_incidents` | Open incidents via EM REST API | 1-N REST |
| `get_zdlra_protected_databases` | ZDLRA protected database list | 1+N REST |

## Requirements

- Oracle Enterprise Manager 24ai (13.5+)
- Python 3.11+
- `uv` package manager (recommended) or `pip`

## Installation

```bash
# Clone the repository
git clone https://github.com/<your-username>/em-mcp-server.git
cd em-mcp-server

# Install dependencies
uv pip install -r requirements.txt
# or
pip install -r requirements.txt
```

## Configuration

Copy the template and fill in your EM details:

```bash
cp config/.env.template config/.env
vi config/.env
```

```ini
EM_BASE_URL=https://your-em-host:7799/em
EM_USER=emmcpuser
EM_PASSWORD=your_password
EM_SSL_VERIFY=false
EM_CA_BUNDLE=
EM_TIMEOUT=60
MCP_PORT=8090
MCP_TRANSPORT=sse
LOG_LEVEL=INFO
```

## EM Prerequisites

Enable the SQL execution REST endpoint on EM OMS (one-time):

```bash
emctl set property \
  -name oracle.sysman.db.restfulapi.executesql.repository.query.enable \
  -value true -sysman_pwd <password>

emctl stop oms -all
emctl start oms
```

## Running the server

```bash
# SSE transport (for AI Client Desktop via mcp-remote)
uv run python em_mcp_server_restapi.py sse

# stdio transport (for direct AI Client Desktop integration)
uv run python em_mcp_server_restapi.py stdio

# Background
nohup uv run python em_mcp_server_restapi.py sse > /tmp/em_mcp.log 2>&1 &
```

## AI Client Desktop Configuration

```json
{
  "mcpServers": {
    "em-mcp-server": {
      "command": "npx",
      "args": [
        "-y",
        "mcp-remote",
        "http://<server-ip>:8090/mcp",
        "--allow-http"
      ]
    }
  }
}
```

## Time Window Format

All performance tools accept flexible time windows:

| Format | Example | Meaning |
|---|---|---|
| Named | `daily` | Last 24 hours |
| Named | `last_7_days` | Last 7 days |
| Hours | `6h` | Last 6 hours |
| Minutes | `30m` | Last 30 minutes |
| Days | `3d` | Last 3 days |
| Override | `6h:raw` | Last 6 hours, raw granularity |

## Target Type Aliases

Common aliases accepted by all tools:

| You say | EM type |
|---|---|
| `database`, `db`, `cdb` | `oracle_database` |
| `rac` | `rac_database` |
| `pdb` | `oracle_pdb` |
| `host` | `host` |
| `exadata_cloud`, `exadb` | `oracle_cloud_vm_cluster` |
| `zdlra` | `oracle_dblra` |
| `exadata` | `oracle_exadata` |

## Sample Questions

**Host:**
- "Show me all host targets and their status"
- "What is the CPU and memory utilization for all hosts in the last 6 hours?"
- "Show details for japacse-xs1 including OS and filesystem usage"

**Database:**
- "Show the database version for aisedb"
- "Show configuration details for aisedb including SGA, PGA and tablespaces"
- "What are the top 5 wait events for aisedb in the last hour?"
- "Show performance metrics for js26ai in the last 3 hours"

**Incidents:**
- "Show all open incidents"
- "Any fatal incidents right now?"
- "Show critical incidents for database targets"

**ZDLRA:**
- "List all protected databases on the ZDLRA"
- "Show ZDLRA protected databases for CDB1DB2"

## License
MIT
