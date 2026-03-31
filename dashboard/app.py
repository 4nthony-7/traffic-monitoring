import os
import json
import queue
import threading
import time
from datetime import datetime

import streamlit as st
import pandas as pd

# ── Page config ────────────────────────────────────────────────────
st.set_page_config(
    page_title="Traffic Control",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ─────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;500&display=swap');

/* Base */
html, body, [data-testid="stAppViewContainer"] {
    background-color: #0a0a0f;
    color: #e2e2e8;
    font-family: 'DM Sans', sans-serif;
}

[data-testid="stSidebar"] {
    background-color: #0f0f17;
    border-right: 1px solid #1e1e2e;
}

[data-testid="stSidebar"] > div:first-child {
    padding-top: 2rem;
}

/* Hide default streamlit elements */
#MainMenu, footer, header { visibility: hidden; }
[data-testid="stToolbar"] { display: none; }

/* Header */
.dash-header {
    display: flex;
    align-items: baseline;
    gap: 1.5rem;
    padding: 0.5rem 0 2rem 0;
    border-bottom: 1px solid #1e1e2e;
    margin-bottom: 2rem;
}
.dash-title {
    font-family: 'Space Mono', monospace;
    font-size: 1.1rem;
    font-weight: 700;
    letter-spacing: 0.15em;
    text-transform: uppercase;
    color: #e2e2e8;
    margin: 0;
}
.dash-subtitle {
    font-size: 0.75rem;
    color: #4a4a6a;
    letter-spacing: 0.08em;
    text-transform: uppercase;
}
.live-dot {
    width: 7px;
    height: 7px;
    background: #00ff88;
    border-radius: 50%;
    display: inline-block;
    margin-right: 0.4rem;
    animation: pulse 2s ease-in-out infinite;
    box-shadow: 0 0 8px #00ff88;
}
@keyframes pulse {
    0%, 100% { opacity: 1; transform: scale(1); }
    50% { opacity: 0.5; transform: scale(0.85); }
}

/* KPI Cards */
.kpi-grid {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 1px;
    background: #1e1e2e;
    border: 1px solid #1e1e2e;
    border-radius: 8px;
    overflow: hidden;
    margin-bottom: 2rem;
}
.kpi-card {
    background: #0f0f17;
    padding: 1.4rem 1.6rem;
    display: flex;
    flex-direction: column;
    gap: 0.3rem;
}
.kpi-label {
    font-size: 0.65rem;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: #4a4a6a;
    font-family: 'Space Mono', monospace;
}
.kpi-value {
    font-family: 'Space Mono', monospace;
    font-size: 2rem;
    font-weight: 700;
    color: #e2e2e8;
    line-height: 1;
}
.kpi-value.accent { color: #ff4466; }
.kpi-value.green  { color: #00ff88; }
.kpi-delta {
    font-size: 0.7rem;
    color: #4a4a6a;
    font-family: 'Space Mono', monospace;
}

/* Section labels */
.section-label {
    font-family: 'Space Mono', monospace;
    font-size: 0.82rem;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: #8888aa;
    margin-bottom: 0.75rem;
    padding-bottom: 0.5rem;
    border-bottom: 1px solid #1e1e2e;
}

/* Violation badge */
.viol-row { border-left: 2px solid #ff4466 !important; }
.ok-row   { border-left: 2px solid #00ff88 !important; }

/* Sidebar stats */
.stat-item {
    padding: 0.9rem 0;
    border-bottom: 1px solid #1e1e2e;
}
.stat-item:last-child { border-bottom: none; }
.stat-name {
    font-size: 0.65rem;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: #4a4a6a;
    font-family: 'Space Mono', monospace;
    margin-bottom: 0.2rem;
}
.stat-val {
    font-family: 'Space Mono', monospace;
    font-size: 0.95rem;
    color: #e2e2e8;
}
.stat-val.red { color: #ff4466; }
.stat-val.green { color: #00ff88; }

/* Dataframe overrides */
[data-testid="stDataFrame"] {
    border: 1px solid #1e1e2e !important;
    border-radius: 6px;
    overflow: hidden;
}
.stDataFrame thead tr th {
    background-color: #0f0f17 !important;
    color: #4a4a6a !important;
    font-family: 'Space Mono', monospace !important;
    font-size: 0.65rem !important;
    text-transform: uppercase !important;
    letter-spacing: 0.1em !important;
    border-bottom: 1px solid #1e1e2e !important;
}
.stDataFrame tbody tr td {
    background-color: #0a0a0f !important;
    color: #c2c2d0 !important;
    font-family: 'DM Sans', sans-serif !important;
    font-size: 0.82rem !important;
    border-bottom: 1px solid #12121e !important;
}
.stDataFrame tbody tr:hover td {
    background-color: #0f0f1a !important;
}

/* Info box */
.info-box {
    background: #0f0f17;
    border: 1px solid #1e1e2e;
    border-left: 3px solid #4a4a6a;
    border-radius: 4px;
    padding: 0.9rem 1rem;
    font-size: 0.8rem;
    color: #4a4a6a;
    font-family: 'Space Mono', monospace;
    letter-spacing: 0.03em;
}

/* Speed bar */
.speed-bar-wrap {
    background: #1e1e2e;
    border-radius: 2px;
    height: 3px;
    margin-top: 0.4rem;
    overflow: hidden;
}
.speed-bar-fill {
    height: 100%;
    border-radius: 2px;
    transition: width 0.4s ease;
}
</style>
""", unsafe_allow_html=True)

# ── Constants ──────────────────────────────────────────────────────
BOOTSTRAP   = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPICS      = ["all_vehicles", "violations", "traffic_stats"]
SPEED_LIMIT = 110
DISTANCE_KM = 3

# ── Session state init ─────────────────────────────────────────────
for key, default in [
    ("data_queue",      None),
    ("vehicles",        []),
    ("violations",      []),
    ("stats",           []),
    ("thread_started",  False),
    ("total_seen",      0),
    ("violation_count", 0),
    ("last_update",     None),
]:
    if key not in st.session_state:
        st.session_state[key] = default

if st.session_state.data_queue is None:
    st.session_state.data_queue = queue.Queue(maxsize=1000)

# ── Kafka thread ───────────────────────────────────────────────────
def kafka_thread(data_queue: queue.Queue):
    try:
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(
            *TOPICS,
            bootstrap_servers=BOOTSTRAP,
            auto_offset_reset="earliest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=1000,
        )
        while True:
            for msg in consumer:
                data_queue.put({"topic": msg.topic, "value": msg.value})
    except Exception as e:
        data_queue.put({"topic": "_error", "value": str(e)})

if not st.session_state.thread_started:
    t = threading.Thread(
        target=kafka_thread,
        args=(st.session_state.data_queue,),
        daemon=True,
        name="kafka_thread",
    )
    t.start()
    st.session_state.thread_started = True

# ── Drain queue ────────────────────────────────────────────────────
drained = 0
while not st.session_state.data_queue.empty() and drained < 150:
    item  = st.session_state.data_queue.get_nowait()
    topic = item["topic"]
    value = item["value"]

    if topic == "_error":
        st.session_state._kafka_error = value
    elif topic == "all_vehicles":
        st.session_state.vehicles.append(value)
        st.session_state.vehicles  = st.session_state.vehicles[-500:]
        st.session_state.total_seen += 1
        st.session_state.last_update = datetime.now().strftime("%H:%M:%S")
    elif topic == "violations":
        st.session_state.violations.append(value)
        st.session_state.violations    = st.session_state.violations[-500:]
        st.session_state.violation_count += 1
    elif topic == "traffic_stats":
        st.session_state.stats.append(value)
        st.session_state.stats = st.session_state.stats[-200:]
    drained += 1

# ── Computed stats ─────────────────────────────────────────────────
veh   = st.session_state.vehicles
viols = st.session_state.violations
stats = st.session_state.stats

total      = len(veh)
viol_count = len(viols)
compliance = round((1 - viol_count / max(total, 1)) * 100, 1)

avg_speed = 0.0
max_speed = 0.0
if veh:
    speeds = [v.get("avg_speed_kmh", 0) for v in veh if v.get("avg_speed_kmh")]
    if speeds:
        avg_speed = round(sum(speeds) / len(speeds), 1)
        max_speed = round(max(speeds), 1)

# ── Sidebar ────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
        <div style='font-family:"Space Mono",monospace;font-size:0.6rem;
                    letter-spacing:0.2em;text-transform:uppercase;
                    color:#4a4a6a;margin-bottom:2rem;'>
            System Status
        </div>
    """, unsafe_allow_html=True)

    # Live indicator
    if st.session_state.last_update:
        status_color = "#00ff88"
        status_text  = "LIVE"
    else:
        status_color = "#4a4a6a"
        status_text  = "WAITING"

    st.markdown(f"""
        <div style='display:flex;align-items:center;gap:0.6rem;margin-bottom:2rem;'>
            <div style='width:7px;height:7px;background:{status_color};
                        border-radius:50%;box-shadow:0 0 8px {status_color};'></div>
            <span style='font-family:"Space Mono",monospace;font-size:0.7rem;
                         color:{status_color};letter-spacing:0.1em;'>{status_text}</span>
        </div>
    """, unsafe_allow_html=True)

    # Session snapshots
    st.markdown("<div class='section-label'>Session</div>", unsafe_allow_html=True)

    sidebar_stats = [
        ("Matched pairs (A+B)", str(total),          ""),
        ("Speed violations",    str(viol_count),      "red" if viol_count > 0 else ""),
        ("Within speed limit",  f"{compliance}%",     "green" if compliance >= 90 else "red"),
        ("Avg speed",           f"{avg_speed} km/h",  "red" if avg_speed > SPEED_LIMIT else ""),
        ("Max speed",           f"{max_speed} km/h",  "red" if max_speed > SPEED_LIMIT else ""),
        ("Last update",         st.session_state.last_update or "—", ""),
    ]

    html_stats = ""
    for label, val, color_cls in sidebar_stats:
        html_stats += f"""
        <div class='stat-item'>
            <div class='stat-name'>{label}</div>
            <div class='stat-val {color_cls}'>{val}</div>
        </div>"""
    st.markdown(html_stats, unsafe_allow_html=True)

    # Traffic stats snapshots
    if stats:
        st.markdown("<div class='section-label' style='margin-top:1.5rem;'>Traffic windows</div>",
                    unsafe_allow_html=True)
        for s in reversed(stats[-8:]):
            start = s.get("window_start", "")[-8:] if s.get("window_start") else "—"
            end   = s.get("window_end",   "")[-8:] if s.get("window_end")   else "—"
            count = s.get("vehicles_at_a", "—")
            st.markdown(f"""
            <div class='stat-item'>
                <div class='stat-name'>{start} → {end}</div>
                <div class='stat-val'>{count} vehicles</div>
            </div>""", unsafe_allow_html=True)
    else:
        st.markdown("<div class='section-label' style='margin-top:1.5rem;'>Traffic windows</div>",
                    unsafe_allow_html=True)
        st.markdown("<div style='font-size:0.72rem;color:#2a2a3e;font-family:\"Space Mono\",monospace;'>No data yet</div>",
                    unsafe_allow_html=True)

    # Config
    st.markdown("<div class='section-label' style='margin-top:1.5rem;'>Configuration</div>",
                unsafe_allow_html=True)
    st.markdown(f"""
    <div class='stat-item'>
        <div class='stat-name'>Speed limit</div>
        <div class='stat-val'>{SPEED_LIMIT} km/h</div>
    </div>
    <div class='stat-item'>
        <div class='stat-name'>Radar distance</div>
        <div class='stat-val'>{DISTANCE_KM} km</div>
    </div>
    <div class='stat-item'>
        <div class='stat-name'>Bootstrap</div>
        <div class='stat-val' style='font-size:0.72rem;color:#4a4a6a;'>{BOOTSTRAP}</div>
    </div>
    """, unsafe_allow_html=True)

# ── Main area ──────────────────────────────────────────────────────

# Header
live_html = '<span class="live-dot"></span>' if st.session_state.last_update else ''
st.markdown(f"""
<div class="dash-header">
    <div>
        <div class="dash-title">{live_html}Highway Speed Control</div>
    </div>
    <div class="dash-subtitle">A{chr(8194)}→{chr(8194)}B{chr(8194)}&nbsp;·&nbsp;
        {DISTANCE_KM} km{chr(8194)}&nbsp;·&nbsp;Limit {SPEED_LIMIT} km/h
    </div>
</div>
""", unsafe_allow_html=True)

# KPI row
kpi_viol_class = "accent" if viol_count > 0 else ""
st.markdown(f"""
<div class="kpi-grid">
    <div class="kpi-card">
        <div class="kpi-label">Matched pairs (A+B)</div>
        <div class="kpi-value green">{total}</div>
        <div class="kpi-delta">vehicles timed between both radars</div>
    </div>
    <div class="kpi-card">
        <div class="kpi-label">Speed violations</div>
        <div class="kpi-value {kpi_viol_class}">{viol_count}</div>
        <div class="kpi-delta">{compliance}% of vehicles within limit</div>
    </div>
    <div class="kpi-card">
        <div class="kpi-label">Avg / Max speed</div>
        <div class="kpi-value">{avg_speed}</div>
        <div class="kpi-delta">max {max_speed} km/h recorded this session</div>
    </div>
</div>
""", unsafe_allow_html=True)

# ── Traffic windows table (full width) ────────────────────────────
st.markdown("<div class='section-label'>Traffic windows</div>", unsafe_allow_html=True)

if stats:
    df_stats = pd.DataFrame(stats[-20:])
    rename = {
        "window_start":   "Window start",
        "window_end":     "Window end",
        "vehicles_at_a":  "Vehicles / window",
    }
    df_stats = df_stats.rename(columns={k: v for k, v in rename.items() if k in df_stats.columns})
    df_stats = df_stats[[c for c in rename.values() if c in df_stats.columns]]
    st.dataframe(df_stats[::-1], use_container_width=True, hide_index=True)
else:
    st.markdown("<div class='info-box'>Waiting for 30-second windows — data appears ~60s after job start</div>",
                unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Violations | Vehicles (side by side) ──────────────────────────
col_viol, col_veh = st.columns(2)

with col_viol:
    st.markdown("<div class='section-label'>Recent violations</div>", unsafe_allow_html=True)
    if viols:
        df_viol = pd.DataFrame(viols[-20:])
        rename_v = {
            "plate":         "Plate",
            "avg_speed_kmh": "Speed (km/h)",
            "excess_kmh":    "Excess (km/h)",
            "fine_eur":      "Fine (EUR)",
            "detected_at":   "Detected at",
        }
        df_viol = df_viol.rename(columns={k: v for k, v in rename_v.items() if k in df_viol.columns})
        df_viol = df_viol[[c for c in rename_v.values() if c in df_viol.columns]]
        st.dataframe(
            df_viol[::-1],
            use_container_width=True,
            hide_index=True,
            column_config={
                "Speed (km/h)": st.column_config.NumberColumn(format="%.1f"),
                "Excess (km/h)": st.column_config.NumberColumn(format="%.1f"),
                "Fine (EUR)": st.column_config.NumberColumn(format="%d €"),
            }
        )
        # Build full violations export (all session data, not just last 20)
        df_export = pd.DataFrame(viols)
        df_export = df_export.rename(columns={k: v for k, v in rename_v.items() if k in df_export.columns})
        df_export = df_export[[c for c in rename_v.values() if c in df_export.columns]]
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        st.download_button(
            label=f"Export all {len(viols)} violations as CSV",
            data=df_export.to_csv(index=False).encode("utf-8"),
            file_name=f"violations_{ts}.csv",
            mime="text/csv",
        )
    else:
        st.markdown("<div class='info-box'>No violations recorded yet</div>",
                    unsafe_allow_html=True)

with col_veh:
    st.markdown("<div class='section-label'>Last vehicles</div>", unsafe_allow_html=True)
    if veh:
        df_veh = pd.DataFrame(veh[-20:])
        rename_veh = {
            "plate":         "Plate",
            "avg_speed_kmh": "Speed (km/h)",
            "violation":     "Violation",
            "detected_at":   "Detected at",
        }
        df_veh = df_veh.rename(columns={k: v for k, v in rename_veh.items() if k in df_veh.columns})
        df_veh = df_veh[[c for c in rename_veh.values() if c in df_veh.columns]]
        st.dataframe(
            df_veh[::-1],
            use_container_width=True,
            hide_index=True,
            column_config={
                "Speed (km/h)": st.column_config.NumberColumn(format="%.1f"),
                "Violation": st.column_config.CheckboxColumn(),
            }
        )
    else:
        st.markdown("<div class='info-box'>Waiting for stream-stream JOIN to produce first pairs (~60s)</div>",
                    unsafe_allow_html=True)

# ── Auto-refresh ───────────────────────────────────────────────────
time.sleep(2)
st.rerun()
