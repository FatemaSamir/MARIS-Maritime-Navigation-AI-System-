"""
streamlit_app.py — Maritime Navigation AI System
Complete dashboard with all 10 features.
Reads from PostgreSQL (Star Schema) + Kafka live feed.
"""
import json, os, sys, random, uuid
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh

sys.path.insert(0, "/app/src")
from common.config import (
    KAFKA_BOOTSTRAP_SERVERS, AIS_TOPIC,
    POSTGRES_URL, MODELS_PATH,
    MAP_DEFAULT_LAT, MAP_DEFAULT_LON, MAP_DEFAULT_ZOOM,
    DASHBOARD_REFRESH_SEC,
)
from ml.scorer import get_scorer

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Maritime Navigation AI",
    page_icon="🚢",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
[data-testid="stMetric"] {
    background:#1C2333; border-radius:10px;
    padding:12px 18px; border:1px solid #2D3748;
}
[data-testid="stMetricLabel"] { color:#A0AEC0 !important; font-size:13px; }
[data-testid="stMetricValue"] { color:#FFFFFF !important; font-size:24px; font-weight:700; }
.alert-high   { background:#7f1d1d; border-left:4px solid #ef4444; padding:8px 12px; border-radius:6px; margin:4px 0; }
.alert-medium { background:#78350f; border-left:4px solid #f59e0b; padding:8px 12px; border-radius:6px; margin:4px 0; }
.alert-low    { background:#14532d; border-left:4px solid #22c55e; padding:8px 12px; border-radius:6px; margin:4px 0; }
</style>
""", unsafe_allow_html=True)

st_autorefresh(interval=DASHBOARD_REFRESH_SEC * 1000, key="main_refresh")

RISK_COLORS = {"HIGH": "#EF5350", "MEDIUM": "#FFA726", "LOW": "#66BB6A"}
DEMO_SHIP_TYPES = ["Cargo","Tanker","Passenger","Fishing","Tug","Military","Sailing"]

VESSEL_TYPE_MAP = {
    "0.0":"Unknown",    "0":"Unknown",
    "1.0":"Reserved",   "1":"Reserved",
    "30.0":"Fishing",   "30":"Fishing",
    "31.0":"Towing",    "31":"Towing",
    "32.0":"Towing Large","32":"Towing Large",
    "35.0":"Military",  "35":"Military",
    "36.0":"Sailing",   "36":"Sailing",
    "37.0":"Pleasure",  "37":"Pleasure",
    "40.0":"High Speed","40":"High Speed",
    "50.0":"Pilot",     "50":"Pilot",
    "51.0":"SAR",       "51":"SAR",
    "52.0":"Tug",       "52":"Tug",
    "55.0":"Law Enf.",  "55":"Law Enf.",
    "60.0":"Passenger", "60":"Passenger",
    "70.0":"Cargo",     "70":"Cargo",
    "71.0":"Cargo-Hazardous","71":"Cargo-Hazardous",
    "80.0":"Tanker",    "80":"Tanker",
    "81.0":"Tanker-Hazardous","81":"Tanker-Hazardous",
    "89.0":"Tanker",    "89":"Tanker",
    "90.0":"Other",     "90":"Other",
}

# ── Session state ──────────────────────────────────────────────────────────────
def _init(k, v):
    if k not in st.session_state:
        st.session_state[k] = v

_init("session_id",       str(uuid.uuid4())[:8])
_init("vessel_store",     {})
_init("vessel_history",   {})
_init("total_consumed",   0)
_init("partition_offsets",{})
_init("demo_mode",        False)
_init("demo_vessels",     {})
_init("alerts_store",     [])


# ── PostgreSQL reader ──────────────────────────────────────────────────────────
@st.cache_data(ttl=5)
def read_pg(query: str, params: dict = None) -> pd.DataFrame:
    """Read from PostgreSQL with 5-second cache."""
    try:
        from sqlalchemy import create_engine, text
        engine = create_engine(POSTGRES_URL, pool_pre_ping=True)
        with engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            return pd.DataFrame(result.fetchall(),
                                columns=result.keys())
    except Exception as e:
        return pd.DataFrame()


# ── Kafka consumer ─────────────────────────────────────────────────────────────
def read_kafka_batch() -> list:
    try:
        from kafka import KafkaConsumer, TopicPartition
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=1500,
            max_poll_records=300,
            session_timeout_ms=30000,
        )
        parts = consumer.partitions_for_topic(AIS_TOPIC)
        if not parts:
            consumer.close()
            return []
        tps = [TopicPartition(AIS_TOPIC, p) for p in sorted(parts)]
        consumer.assign(tps)
        for tp in tps:
            saved = st.session_state.partition_offsets.get(tp.partition)
            if saved is not None:
                consumer.seek(tp, saved)
            else:
                consumer.seek_to_beginning(tp)
        records = []
        for msg in consumer:
            records.append(msg.value)
            st.session_state.partition_offsets[msg.partition] = msg.offset + 1
            if len(records) >= 300:
                break
        consumer.close()
        if records:
            st.session_state["demo_mode"] = False
        return records
    except Exception:
        st.session_state["demo_mode"] = True
        return []


# ── Demo data generator ────────────────────────────────────────────────────────
def generate_demo(n: int = 20) -> list:
    state = st.session_state.demo_vessels
    if not state:
        for i in range(40):
            mmsi = str(300_000_000 + i)
            state[mmsi] = {
                "mmsi": mmsi,
                "vessel_name": f"VESSEL-{i:03d}",
                "vessel_type": random.choice(DEMO_SHIP_TYPES),
                "lat": MAP_DEFAULT_LAT + random.uniform(-5, 5),
                "lon": MAP_DEFAULT_LON + random.uniform(-5, 5),
                "sog": random.uniform(0, 20),
                "cog": random.uniform(0, 360),
                "heading": random.uniform(0, 360),
            }
    keys  = random.sample(list(state.keys()), min(n, len(state)))
    now   = datetime.utcnow().isoformat(timespec="seconds")
    batch = []
    for mmsi in keys:
        v = state[mmsi]
        v["lat"]     = max(-80, min(80,  v["lat"] + random.uniform(-0.03, 0.03)))
        v["lon"]     = max(-170, min(170, v["lon"] + random.uniform(-0.03, 0.03)))
        v["sog"]     = max(0, min(25, v["sog"] + random.uniform(-0.5, 0.5)))
        v["heading"] = (v["heading"] + random.uniform(-5, 5)) % 360
        rec = {**v, "base_datetime": now, "data_split": "live"}
        rec["risk_level"] = (
            "HIGH"   if v["sog"] < 1 and 29.5 <= v["lat"] <= 31.5 else
            "MEDIUM" if v["sog"] < 2 else "LOW"
        )
        batch.append(rec)
    return batch


def merge_records(records: list):
    scorer = get_scorer()
    for rec in records:
        mmsi = str(rec.get("mmsi", "")).strip()
        if not mmsi:
            continue
        st.session_state.vessel_store[mmsi] = rec
        st.session_state.vessel_history.setdefault(mmsi, []).append(rec)
    st.session_state.total_consumed += len(records)


def store_to_df() -> pd.DataFrame:
    if not st.session_state.vessel_store:
        return pd.DataFrame()
    df = pd.DataFrame(st.session_state.vessel_store.values())
    for c in ["lat","lon","sog","cog","heading"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "vessel_type" in df.columns:
        df["vessel_type"] = df["vessel_type"].apply(
            lambda x: VESSEL_TYPE_MAP.get(str(x).strip(), str(x).strip())
            if not (x is None or (isinstance(x, float) and pd.isna(x))) else "Unknown"
        )
    return df


def get_track(mmsi: str) -> pd.DataFrame:
    hist = st.session_state.vessel_history.get(mmsi, [])
    if not hist:
        return pd.DataFrame()
    df = pd.DataFrame(hist)
    for c in ["lat","lon","sog","cog","heading"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "base_datetime" in df.columns:
        df["base_datetime"] = pd.to_datetime(df["base_datetime"], errors="coerce")
        df = df.sort_values("base_datetime")
    return df.dropna(subset=["lat","lon"]).reset_index(drop=True)


# ── Pull live data ─────────────────────────────────────────────────────────────
new_recs = read_kafka_batch()
if st.session_state["demo_mode"]:
    new_recs = generate_demo(20)
if new_recs:
    merge_records(new_recs)

data = store_to_df()

# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("🚢 Maritime AI")

    if st.session_state["demo_mode"]:
        st.warning("⚠️ DEMO MODE\nKafka unavailable")

    page = st.radio("Navigation", [
        "🗺️ Live Vessel Map",
        "⏮️ Historical Replay",
        "🔥 Traffic Heatmap",
        "⚠️ Anomaly Detection",
        "🚨 Collision Risk",
        "🔔 Alerts",
        "📊 Analytics",
        "🔍 Search",
    ])

    st.divider()

    # Filters
    st.subheader("Filters")
    sog_max      = max(30.0, float(data["sog"].max()) if not data.empty and "sog" in data.columns else 30.0)
    speed_range  = st.slider("Speed (SOG) kn", 0.0, sog_max, (0.0, sog_max), 0.5)
    risk_filter  = st.multiselect("Risk Level", ["HIGH","MEDIUM","LOW"], default=["HIGH","MEDIUM","LOW"])
    all_types    = sorted(data["vessel_type"].dropna().unique().tolist()) if not data.empty and "vessel_type" in data.columns else []
    type_filter  = st.multiselect("Vessel Type", all_types)

    st.divider()
    st.caption(f"Session: `{st.session_state.session_id}`")
    st.caption(f"Vessels: `{len(st.session_state.vessel_store)}`")
    st.caption(f"Records: `{st.session_state.total_consumed:,}`")
    if st.button("🗑️ Reset"):
        for k in ["vessel_store","vessel_history","partition_offsets",
                  "demo_vessels","total_consumed","alerts_store"]:
            st.session_state[k] = {} if k != "total_consumed" else 0
        st.rerun()


def apply_filters(df):
    if df.empty:
        return df
    if "sog" in df.columns:
        df = df[df["sog"].between(speed_range[0], speed_range[1])]
    if "risk_level" in df.columns and risk_filter:
        df = df[df["risk_level"].isin(risk_filter)]
    if "vessel_type" in df.columns and type_filter:
        df = df[df["vessel_type"].isin(type_filter)]
    return df


filtered = apply_filters(data.copy()) if not data.empty else data


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — Live Vessel Map
# ═══════════════════════════════════════════════════════════════════════════════
if "Live Vessel Map" in page:
    st.title("📍 Live Vessel Map")

    if not data.empty:
        c1,c2,c3,c4,c5 = st.columns(5)
        c1.metric("Vessels",       f"{len(data):,}")
        c2.metric("Filtered",      f"{len(filtered):,}")
        c3.metric("New Records",   f"{len(new_recs):,}")
        c4.metric("Total Consumed",f"{st.session_state.total_consumed:,}")
        if "risk_level" in data.columns:
            rc = data["risk_level"].value_counts()
            c5.metric("🔴 HIGH Risk", int(rc.get("HIGH",0)))

        if "risk_level" in data.columns:
            r1,r2,r3 = st.columns(3)
            rc = data["risk_level"].value_counts()
            r1.metric("🔴 HIGH",   int(rc.get("HIGH",0)))
            r2.metric("🟡 MEDIUM", int(rc.get("MEDIUM",0)))
            r3.metric("🟢 LOW",    int(rc.get("LOW",0)))

        st.divider()

    if not filtered.empty and {"lat","lon"}.issubset(filtered.columns):
        map_df = filtered.dropna(subset=["lat","lon"]).copy()
        latest = map_df.drop_duplicates("mmsi", keep="last").rename(
            columns={"lat":"latitude","lon":"longitude"})

        hover = [c for c in ["vessel_name","vessel_type","sog",
                              "heading","risk_level","is_anomaly",
                              "base_datetime"] if c in latest.columns]

        if "risk_level" in latest.columns:
            fig = px.scatter_mapbox(
                latest, lat="latitude", lon="longitude",
                hover_name="mmsi", hover_data=hover,
                color="risk_level", color_discrete_map=RISK_COLORS,
                zoom=MAP_DEFAULT_ZOOM, height=580,
            )
        else:
            fig = px.scatter_mapbox(
                latest, lat="latitude", lon="longitude",
                hover_name="mmsi", hover_data=hover,
                color="sog" if "sog" in latest.columns else None,
                color_continuous_scale="Plasma",
                zoom=MAP_DEFAULT_ZOOM, height=580,
            )

        fig.update_layout(
            mapbox_style="open-street-map",
            margin={"r":0,"t":0,"l":0,"b":0},
            paper_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig, use_container_width=True)
        st.caption(f"Showing {latest['mmsi'].nunique():,} vessels")
    else:
        st.warning("⚠️ No data. Start the Kafka producer:\n```\ndocker compose exec producer python src/producer/kafka_producer.py\n```")


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — Historical Replay
# ═══════════════════════════════════════════════════════════════════════════════
elif "Historical Replay" in page:
    st.title("⏮️ Historical Replay")

    col1, col2 = st.columns([2,1])
    with col1:
        mmsi_replay = st.text_input("MMSI to replay", placeholder="e.g. 368000000")
    with col2:
        replay_split = st.selectbox("Data split", ["live","test","train","all"])

    if mmsi_replay:
        tdf = get_track(mmsi_replay)
        if tdf.empty:
            # Try PostgreSQL
            split_filter = "" if replay_split == "all" else f"AND data_split='{replay_split}'"
            pg_df = read_pg(f"""
                SELECT lat, lon, sog, cog, heading, risk_level,
                       is_anomaly, base_datetime
                FROM fact_ais_track
                WHERE mmsi = '{mmsi_replay}' {split_filter}
                ORDER BY base_datetime
                LIMIT 5000
            """)
            tdf = pg_df if not pg_df.empty else tdf

        if not tdf.empty:
            v_name = str(tdf["vessel_name"].iloc[-1]) if "vessel_name" in tdf.columns else mmsi_replay
            st.subheader(f"Track: {v_name} ({mmsi_replay})")

            if "base_datetime" in tdf.columns:
                tdf["base_datetime"] = pd.to_datetime(tdf["base_datetime"], errors="coerce")
                tdf = tdf.sort_values("base_datetime")

            frame = st.slider("Timeline", 0, max(len(tdf)-1, 1),
                              max(len(tdf)-1, 1))
            curr = tdf.iloc[frame]

            s1,s2,s3,s4 = st.columns(4)
            s1.metric("Frame",  f"{frame+1}/{len(tdf)}")
            s2.metric("SOG",    f"{curr.get('sog',0):.1f} kn")
            s3.metric("Risk",   curr.get("risk_level","—"))
            s4.metric("Time",   str(curr.get("base_datetime",""))[:19])

            fig = go.Figure()
            fig.add_trace(go.Scattermapbox(
                lat=tdf["lat"].tolist(), lon=tdf["lon"].tolist(),
                mode="lines", line=dict(width=2, color="#90A4AE"),
                name="Full Route",
            ))
            fig.add_trace(go.Scattermapbox(
                lat=[curr["lat"]], lon=[curr["lon"]],
                mode="markers",
                marker=dict(size=16, color="#FF7043"),
                name="Current Position",
            ))
            fig.update_layout(
                mapbox=dict(
                    style="open-street-map",
                    center=dict(lat=float(curr["lat"]),
                                lon=float(curr["lon"])),
                    zoom=9,
                ),
                margin={"r":0,"t":0,"l":0,"b":0},
                height=520,
                paper_bgcolor="rgba(0,0,0,0)",
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No track data. Start producer and wait for data.")


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — Traffic Heatmap
# ═══════════════════════════════════════════════════════════════════════════════
elif "Traffic Heatmap" in page:
    st.title("🔥 Traffic Density Heatmap")

    # Try PostgreSQL first, fallback to live data
    density_df = read_pg("""
        SELECT lat_bin AS lat, lon_bin AS lon,
               vessel_count, avg_sog, congestion_level
        FROM fact_traffic_density
        WHERE hour_bucket >= NOW() - INTERVAL '24 hours'
        ORDER BY vessel_count DESC
        LIMIT 1000
    """)

    if density_df.empty and not filtered.empty:
        # Build from live Kafka data
        density_df = (
            filtered.assign(
                lat_bin=(filtered["lat"] / 0.5).astype(int) * 0.5,
                lon_bin=(filtered["lon"] / 0.5).astype(int) * 0.5,
            )
            .groupby(["lat_bin","lon_bin"])
            .agg(vessel_count=("mmsi","nunique"), avg_sog=("sog","mean"))
            .reset_index()
            .rename(columns={"lat_bin":"lat","lon_bin":"lon"})
        )
        density_df["congestion_level"] = density_df["vessel_count"].apply(
            lambda n: "HIGH" if n>=10 else ("MEDIUM" if n>=5 else "LOW")
        )

    if not density_df.empty:
        h1,h2,h3 = st.columns(3)
        h1.metric("Grid Cells", f"{len(density_df):,}")
        if "congestion_level" in density_df.columns:
            vc = density_df["congestion_level"].value_counts()
            h2.metric("HIGH Zones",   int(vc.get("HIGH",0)))
            h3.metric("MEDIUM Zones", int(vc.get("MEDIUM",0)))

        density_df["weight"] = (
            density_df["vessel_count"] /
            density_df["vessel_count"].max()
        ).clip(0, 1)

        fig = px.density_mapbox(
            density_df, lat="lat", lon="lon",
            z="vessel_count", radius=30,
            color_continuous_scale="YlOrRd",
            mapbox_style="open-street-map",
            zoom=MAP_DEFAULT_ZOOM, height=560,
            hover_data=["vessel_count","avg_sog",
                        "congestion_level"] if "congestion_level" in density_df.columns else ["vessel_count","avg_sog"],
        )
        fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0},
                          paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Collecting density data...")


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 4 — Anomaly Detection
# ═══════════════════════════════════════════════════════════════════════════════
elif "Anomaly" in page:
    st.title("⚠️ Anomaly Detection")

    anomalies_df = read_pg("""
        SELECT mmsi_1 AS mmsi, vessel_name_1 AS vessel_name,
               alert_type, severity, lat, lon,
               anomaly_score, description, created_at
        FROM fact_alerts
        WHERE alert_type IN ('SUDDEN_STOP','SHARP_TURN',
              'UNUSUAL_SPEED','STATIONARY_RISK','ML_ANOMALY')
        AND created_at >= NOW() - INTERVAL '24 hours'
        ORDER BY anomaly_score DESC, created_at DESC
        LIMIT 200
    """)

    # Also check live data for anomalies
    if not filtered.empty and "is_anomaly" in filtered.columns:
        live_anom = filtered[filtered["is_anomaly"] == True]
        if not live_anom.empty:
            st.subheader(f"🔴 Live Anomalies: {len(live_anom)}")
            for _, row in live_anom.head(5).iterrows():
                atype = row.get("anomaly_type","ANOMALY")
                st.markdown(
                    f"<div class='alert-high'>"
                    f"<b>MMSI {row['mmsi']}</b> — {row.get('vessel_name','')}<br/>"
                    f"Type: {atype} | SOG: {row.get('sog',0):.1f} kn | "
                    f"Risk: {row.get('risk_level','—')}"
                    f"</div>",
                    unsafe_allow_html=True,
                )

    st.divider()
    if not anomalies_df.empty:
        st.subheader(f"Historical Anomalies ({len(anomalies_df)})")

        # Anomaly type distribution
        type_counts = anomalies_df["alert_type"].value_counts().reset_index()
        type_counts.columns = ["type", "count"]
        fig_bar = px.bar(type_counts, x="count", y="type",
                         orientation="h", color="count",
                         color_continuous_scale="Reds",
                         template="plotly_dark")
        fig_bar.update_layout(paper_bgcolor="rgba(0,0,0,0)",
                               plot_bgcolor="rgba(0,0,0,0)",
                               showlegend=False,
                               coloraxis_showscale=False)
        st.plotly_chart(fig_bar, use_container_width=True)

        st.dataframe(anomalies_df, use_container_width=True, height=350)
    else:
        st.info("No historical anomalies yet. Run training and live streaming.")


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 5 — Collision Risk
# ═══════════════════════════════════════════════════════════════════════════════
elif "Collision" in page:
    st.title("🚨 Collision Risk Detection")

    # Live collision detection from current vessel positions
    if not filtered.empty and len(filtered) > 1:
        scorer = get_scorer()
        latest = filtered.drop_duplicates("mmsi", keep="last")
        risks  = scorer.detect_collisions(latest)

        if risks:
            st.error(f"⚠️ {len(risks)} collision risk(s) detected!")
            for r in risks[:10]:
                color = "alert-high" if r["severity"] in ("CRITICAL","HIGH") else "alert-medium"
                st.markdown(
                    f"<div class='{color}'>"
                    f"<b>🚨 {r['severity']}</b> — "
                    f"{r.get('vessel_1') or r['mmsi_1']} ↔ "
                    f"{r.get('vessel_2') or r['mmsi_2']}<br/>"
                    f"Distance: {r['distance_nm']:.3f} nm | "
                    f"Converging: {'Yes' if r['converging'] else 'No'}"
                    f"</div>",
                    unsafe_allow_html=True,
                )

            # Show on map
            risk_points = []
            for r in risks:
                risk_points.append({
                    "lat": r["lat"], "lon": r["lon"],
                    "severity": r["severity"],
                    "label": f"{r['mmsi_1']}↔{r['mmsi_2']}",
                    "distance": r["distance_nm"],
                })
            rdf = pd.DataFrame(risk_points)
            fig = px.scatter_mapbox(
                rdf, lat="lat", lon="lon",
                hover_name="label",
                hover_data=["severity","distance"],
                color="severity",
                color_discrete_map=RISK_COLORS,
                size_max=20,
                zoom=MAP_DEFAULT_ZOOM, height=480,
            )
            fig.update_layout(
                mapbox_style="open-street-map",
                margin={"r":0,"t":0,"l":0,"b":0},
                paper_bgcolor="rgba(0,0,0,0)",
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.success("✅ No collision risks detected in current data.")
    else:
        st.info("Need live vessel data. Start the producer.")


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 6 — Alerts
# ═══════════════════════════════════════════════════════════════════════════════
elif "Alerts" in page:
    st.title("🔔 Maritime Alerts")

    alerts_df = read_pg("""
        SELECT id, alert_type, severity,
               mmsi_1 AS mmsi, vessel_name_1 AS vessel,
               description, is_resolved, created_at
        FROM fact_alerts
        ORDER BY created_at DESC
        LIMIT 500
    """)

    if not alerts_df.empty:
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Alerts", len(alerts_df))
        col2.metric("Critical", int((alerts_df["severity"]=="CRITICAL").sum()))
        col3.metric("HIGH",     int((alerts_df["severity"]=="HIGH").sum()))
        col4.metric("Resolved", int(alerts_df["is_resolved"].sum()) if "is_resolved" in alerts_df.columns else 0)

        st.divider()

        for _, row in alerts_df.head(50).iterrows():
            sev   = str(row.get("severity","LOW"))
            css   = ("alert-high" if sev in ("CRITICAL","HIGH")
                     else "alert-medium" if sev=="MEDIUM"
                     else "alert-low")
            emoji = ("🔴" if sev in ("CRITICAL","HIGH")
                     else "🟡" if sev == "MEDIUM" else "🟢")
            st.markdown(
                f"<div class='{css}'>"
                f"{emoji} <b>{sev}</b> [{row['alert_type']}] — "
                f"{row.get('vessel') or row.get('mmsi','?')}<br/>"
                f"<small>{row.get('description','')}</small>"
                f"</div>",
                unsafe_allow_html=True,
            )
    else:
        if not filtered.empty and "risk_level" in filtered.columns:
            high = filtered[filtered["risk_level"]=="HIGH"]
            if not high.empty:
                st.warning(f"⚠️ {len(high)} HIGH risk vessels in live feed (not yet stored to DB)")
                st.dataframe(high[["mmsi","vessel_name","lat","lon",
                                   "sog","risk_level"]].head(20),
                             use_container_width=True)
        else:
            st.info("No alerts yet. Run the full pipeline.")


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 7 — Analytics
# ═══════════════════════════════════════════════════════════════════════════════
elif "Analytics" in page:
    st.title("📊 Analytics Dashboard")
    COLORS = ["#42A5F5","#FF7043","#AB47BC","#26A69A","#FFA726","#8D6E63"]

    # KPIs
    k1,k2,k3,k4 = st.columns(4)
    k1.metric("Live Vessels",   f"{len(data):,}")
    k2.metric("Unique Tracked", f"{len(st.session_state.vessel_store):,}")
    k3.metric("Total Records",  f"{st.session_state.total_consumed:,}")
    if not data.empty and "sog" in data.columns:
        k4.metric("Avg SOG", f"{data['sog'].mean():.1f} kn")

    st.divider()

    col1, col2 = st.columns(2)

    # Speed histogram
    with col1:
        st.subheader("Speed Distribution")
        if not filtered.empty and "sog" in filtered.columns:
            fig = px.histogram(
                filtered.dropna(subset=["sog"]),
                x="sog", nbins=30,
                color_discrete_sequence=["#3182CE"],
                template="plotly_dark",
                labels={"sog":"Speed Over Ground (knots)"},
            )
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)",
                              plot_bgcolor="rgba(0,0,0,0)", bargap=0.05)
            st.plotly_chart(fig, use_container_width=True)

    # Vessel type pie
    with col2:
        st.subheader("Vessel Types")
        if not filtered.empty and "vessel_type" in filtered.columns:
            tc = (filtered["vessel_type"].replace("","Unknown")
                  .fillna("Unknown").value_counts().head(8)
                  .reset_index())
            tc.columns = ["type","count"]
            fig = px.pie(tc, names="type", values="count",
                         color_discrete_sequence=COLORS,
                         hole=0.4, template="plotly_dark")
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig, use_container_width=True)

    # Risk breakdown
    col3, col4 = st.columns(2)
    with col3:
        st.subheader("Risk Level Breakdown")
        if not filtered.empty and "risk_level" in filtered.columns:
            rc = filtered["risk_level"].value_counts().reset_index()
            rc.columns = ["risk","count"]
            fig = px.pie(rc, names="risk", values="count",
                         color="risk", color_discrete_map=RISK_COLORS,
                         hole=0.45, template="plotly_dark")
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig, use_container_width=True)

    # Daily stats from DB
    with col4:
        st.subheader("Daily Vessel Count")
        daily_df = read_pg("""
            SELECT stat_date, total_vessels, avg_sog,
                   high_risk_count, anomaly_count
            FROM fact_daily_stats
            ORDER BY stat_date
        """)
        if not daily_df.empty:
            fig = px.line(daily_df, x="stat_date",
                          y="total_vessels",
                          template="plotly_dark",
                          color_discrete_sequence=["#42A5F5"])
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)",
                              plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Run gold_job.py to populate daily stats.")


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 8 — Search
# ═══════════════════════════════════════════════════════════════════════════════
elif "Search" in page:
    st.title("🔍 Vessel Search")

    query = st.text_input("Search MMSI, vessel name, IMO, or call sign",
                          placeholder="e.g. 368000000 or LIBERTY JANE")

    col1, col2, col3 = st.columns(3)
    with col1:
        f_type  = st.selectbox("Vessel Type", ["All"] + all_types)
    with col2:
        f_risk  = st.selectbox("Risk Level", ["All","HIGH","MEDIUM","LOW"])
    with col3:
        f_split = st.selectbox("Data Split", ["All","live","test","train"])

    if query and len(query) >= 2:
        # Search in live data first
        results = pd.DataFrame()
        if not data.empty:
            mask = (
                data["mmsi"].astype(str).str.contains(query, case=False, na=False) |
                data.get("vessel_name", pd.Series(dtype=str)).astype(str).str.contains(query, case=False, na=False)
            )
            results = data[mask].copy()

        if results.empty:
            # Search PostgreSQL
            results = read_pg(f"""
                SELECT v.mmsi, v.vessel_name, v.vessel_type,
                       v.lat, v.lon, v.sog, v.risk_level,
                       v.is_anomaly, v.updated_at
                FROM fact_vessel_latest v
                WHERE v.mmsi ILIKE '%{query}%'
                   OR v.vessel_name ILIKE '%{query}%'
                LIMIT 100
            """)

        if f_type != "All" and "vessel_type" in results.columns:
            results = results[results["vessel_type"] == f_type]
        if f_risk != "All" and "risk_level" in results.columns:
            results = results[results["risk_level"] == f_risk]

        if not results.empty:
            st.success(f"Found {len(results)} vessel(s)")
            show_cols = [c for c in ["mmsi","vessel_name","vessel_type",
                                      "lat","lon","sog","risk_level",
                                      "is_anomaly","updated_at"]
                         if c in results.columns]
            st.dataframe(results[show_cols], use_container_width=True)

            # Show on map
            if {"lat","lon"}.issubset(results.columns):
                map_r = results.dropna(subset=["lat","lon"]).rename(
                    columns={"lat":"latitude","lon":"longitude"})
                fig = px.scatter_mapbox(
                    map_r, lat="latitude", lon="longitude",
                    hover_name="mmsi",
                    hover_data=[c for c in ["vessel_name","sog","risk_level"]
                                if c in map_r.columns],
                    color="risk_level" if "risk_level" in map_r.columns else None,
                    color_discrete_map=RISK_COLORS,
                    zoom=6, height=400,
                )
                fig.update_layout(
                    mapbox_style="open-street-map",
                    margin={"r":0,"t":0,"l":0,"b":0},
                    paper_bgcolor="rgba(0,0,0,0)",
                )
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning(f"No vessels found for '{query}'")
