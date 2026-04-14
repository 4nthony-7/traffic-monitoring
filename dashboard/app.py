#!/usr/bin/env python3
__author__ = "Anthony Poher"
__date__ = "2026-04-07"

"""
Application: Highway Speed Monitoring Dashboard
--------------------------------
This script implements a Streamlit dashboard to visualize real-time speed control data on a highway.
It consumes data from three Kafka topics: 
                                                - 'all_vehicles'
                                                - 'violations'
                                                - 'traffic_stats'
Viewing the dashboard: open http://localhost:8501 in a browser
"""

import os
import json
import streamlit as st
import pandas as pd

import queue
import threading

import time
from datetime import datetime


# ── Page config ────────────────────────────────────────────────────
st.set_page_config(
    page_title="Traffic Control",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ─────────────────────────────────────────────────────
def local_css(file_name):
    with open(file_name) as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

local_css('style.css')
st.markdown('<div class="dash-header"><h1 class="dash-title">Dashboard</h1></div>', unsafe_allow_html=True)


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
