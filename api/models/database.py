"""
database.py — Maritime Navigation AI System
Star Schema implementation using SQLAlchemy ORM.

DIMENSION TABLES (slowly changing, descriptive data):
  dim_vessel    → who is the vessel
  dim_time      → when did it happen
  dim_location  → where did it happen
  dim_status    → what navigation status

FACT TABLES (measurements, events, aggregations):
  fact_vessel_latest   → current position per vessel (live map)
  fact_ais_track       → full position history (replay)
  fact_traffic_density → vessel density per grid per hour (heatmap)
  fact_alerts          → all system alerts
  fact_predictions     → ML position predictions
  fact_daily_stats     → daily aggregated analytics
"""
from datetime import datetime
from sqlalchemy import (
    create_engine, Column, String, Float, Integer,
    Boolean, DateTime, Date, Text, BigInteger,
    SmallInteger, JSON, Index, UniqueConstraint,
    ForeignKey,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
import os, sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src", "common"))
from config import POSTGRES_URL

Base = declarative_base()


# ═══════════════════════════════════════════════════════════════════════════════
# DIMENSION TABLES
# ═══════════════════════════════════════════════════════════════════════════════

class DimVessel(Base):
    """
    Dimension: Vessel reference data.
    Slowly changing — vessel dimensions don't change often.
    Updated via UPSERT when new info arrives.
    """
    __tablename__ = "dim_vessel"

    mmsi              = Column(String(20),  primary_key=True)
    vessel_name       = Column(String(100), index=True)
    imo               = Column(String(20),  index=True)
    call_sign         = Column(String(20))
    vessel_type       = Column(String(50),  index=True)
    vessel_type_label = Column(String(100)) # human-readable
    length            = Column(Float)
    width             = Column(Float)
    draft             = Column(Float)
    cargo             = Column(String(50))
    transceiver_class = Column(String(10))
    data_split        = Column(String(20))  # train/validation/test/live
    first_seen        = Column(DateTime)
    last_seen         = Column(DateTime)
    total_records     = Column(BigInteger,  default=0)
    created_at        = Column(DateTime,    default=datetime.utcnow)
    updated_at        = Column(DateTime,    default=datetime.utcnow,
                               onupdate=datetime.utcnow)


class DimStatus(Base):
    """
    Dimension: AIS navigation status codes.
    Small table — only ~16 rows.
    """
    __tablename__ = "dim_status"

    status_id    = Column(Integer,     primary_key=True)
    status_code  = Column(String(10),  nullable=False, unique=True)
    status_label = Column(String(100), nullable=False)
    is_underway  = Column(Boolean,     default=False)
    is_stopped   = Column(Boolean,     default=False)
    is_anchored  = Column(Boolean,     default=False)
    is_moored    = Column(Boolean,     default=False)
    is_aground   = Column(Boolean,     default=False)


# ═══════════════════════════════════════════════════════════════════════════════
# FACT TABLES
# ═══════════════════════════════════════════════════════════════════════════════

class FactVesselLatest(Base):
    """
    Fact: Latest known position for each vessel.
    One row per MMSI — UPSERT on every new message.
    This is what the LIVE MAP reads.
    Powers: Feature 1 (Vessel Tracking Map)
    """
    __tablename__ = "fact_vessel_latest"

    mmsi           = Column(String(20),  primary_key=True)
    vessel_name    = Column(String(100), index=True)
    vessel_type    = Column(String(50),  index=True)
    lat            = Column(Float,       nullable=False)
    lon            = Column(Float,       nullable=False)
    sog            = Column(Float)
    cog            = Column(Float)
    heading        = Column(Float)
    risk_level     = Column(String(10),  index=True)
    is_anomaly     = Column(Boolean,     default=False, index=True)
    anomaly_score  = Column(Float,       default=0.0)
    anomaly_type   = Column(String(100))
    predicted_lat  = Column(Float)       # ML prediction
    predicted_lon  = Column(Float)
    base_datetime  = Column(DateTime,    index=True)
    updated_at     = Column(DateTime,    default=datetime.utcnow,
                            onupdate=datetime.utcnow)
    data_split     = Column(String(20))  # live/test/etc

    __table_args__ = (
        Index("ix_latest_lat_lon",    "lat", "lon"),
        Index("ix_latest_risk",       "risk_level", "is_anomaly"),
        Index("ix_latest_updated",    "updated_at"),
    )


class FactAisTrack(Base):
    """
    Fact: Complete AIS position history.
    Every position for every vessel.
    Powers: Feature 2 (Historical Replay), Feature 7 (Collision Risk)
    Partitioned logically by data_split for ML training isolation.
    """
    __tablename__ = "fact_ais_track"

    id             = Column(BigInteger,  primary_key=True,
                            autoincrement=True)
    mmsi           = Column(String(20),  nullable=False, index=True)
    vessel_name    = Column(String(100))
    vessel_type    = Column(String(50),  index=True)
    lat            = Column(Float,       nullable=False)
    lon            = Column(Float,       nullable=False)
    lat_bin        = Column(Float,       index=True)
    lon_bin        = Column(Float,       index=True)
    sog            = Column(Float)
    cog            = Column(Float)
    heading        = Column(Float)
    status         = Column(String(20))
    risk_level     = Column(String(10),  index=True)
    is_anomaly     = Column(Boolean,     default=False)
    anomaly_score  = Column(Float,       default=0.0)
    anomaly_type   = Column(String(100))
    base_datetime  = Column(DateTime,    nullable=False, index=True)
    ingested_at    = Column(DateTime,    default=datetime.utcnow)
    data_split     = Column(String(20),  nullable=False, index=True,
                            server_default="train")

    __table_args__ = (
        Index("ix_track_mmsi_time",   "mmsi", "base_datetime"),
        Index("ix_track_split_time",  "data_split", "base_datetime"),
        Index("ix_track_grid",        "lat_bin", "lon_bin",
              "base_datetime"),
    )


class FactTrafficDensity(Base):
    """
    Fact: Vessel density per grid cell per hour.
    Pre-aggregated for fast heatmap rendering.
    Powers: Feature 3 (Traffic Density Heatmap),
            Feature 4 (Congestion Detection)
    """
    __tablename__ = "fact_traffic_density"

    id               = Column(Integer,  primary_key=True,
                               autoincrement=True)
    lat_bin          = Column(Float,    nullable=False)
    lon_bin          = Column(Float,    nullable=False)
    hour_bucket      = Column(DateTime, nullable=False, index=True)
    vessel_count     = Column(Integer,  nullable=False)
    unique_vessels   = Column(Integer)
    avg_sog          = Column(Float)
    stopped_count    = Column(Integer,  default=0)
    cargo_count      = Column(Integer,  default=0)
    tanker_count     = Column(Integer,  default=0)
    congestion_level = Column(String(10), index=True)
    data_split       = Column(String(20))
    created_at       = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("lat_bin", "lon_bin", "hour_bucket",
                         name="uq_density_grid_hour"),
        Index("ix_density_grid",    "lat_bin", "lon_bin"),
        Index("ix_density_time",    "hour_bucket", "congestion_level"),
    )


class FactAlert(Base):
    """
    Fact: All system-generated maritime alerts.
    Powers: Feature 8 (Maritime Alerts System)
    """
    __tablename__ = "fact_alerts"

    id             = Column(Integer,   primary_key=True,
                            autoincrement=True)
    alert_type     = Column(String(50),  nullable=False, index=True)
    severity       = Column(String(20),  nullable=False, index=True)
    mmsi_1         = Column(String(20),  index=True)
    mmsi_2         = Column(String(20))  # for collision alerts
    vessel_name_1  = Column(String(100))
    vessel_name_2  = Column(String(100))
    lat            = Column(Float)
    lon            = Column(Float)
    description    = Column(Text)
    extra_data     = Column(JSON)
    anomaly_score  = Column(Float)
    distance_nm    = Column(Float)       # for collision alerts
    is_resolved    = Column(Boolean,     default=False, index=True)
    created_at     = Column(DateTime,    default=datetime.utcnow,
                            index=True)
    resolved_at    = Column(DateTime)
    data_split     = Column(String(20),  index=True)

    __table_args__ = (
        Index("ix_alert_type_time",  "alert_type", "created_at"),
        Index("ix_alert_severity",   "severity", "is_resolved"),
        Index("ix_alert_mmsi",       "mmsi_1", "created_at"),
    )



class FactDailyStats(Base):
    """
    Fact: Daily aggregated statistics for analytics charts.
    Powers: Feature 9 (Analytics Dashboard)
    """
    __tablename__ = "fact_daily_stats"

    id                = Column(Integer,     primary_key=True,
                                autoincrement=True)
    stat_date         = Column(Date,        nullable=False,
                                unique=True, index=True)
    total_vessels     = Column(Integer,     default=0)
    total_records     = Column(BigInteger,  default=0)
    avg_sog           = Column(Float)
    max_sog           = Column(Float)
    stopped_vessels   = Column(Integer,     default=0)
    high_risk_count   = Column(Integer,     default=0)
    medium_risk_count = Column(Integer,     default=0)
    anomaly_count     = Column(Integer,     default=0)
    collision_alerts  = Column(Integer,     default=0)
    congestion_hours  = Column(Integer,     default=0)
    top_vessel_type   = Column(String(50))
    data_split        = Column(String(20))
    created_at        = Column(DateTime,    default=datetime.utcnow)


# ═══════════════════════════════════════════════════════════════════════════════
# DB ENGINE + SESSION
# ═══════════════════════════════════════════════════════════════════════════════

def get_engine(url: str = None):
    return create_engine(
        url or POSTGRES_URL,
        pool_size=20,
        max_overflow=40,
        pool_pre_ping=True,
        echo=False,
    )


def get_session(engine=None):
    eng = engine or get_engine()
    return sessionmaker(bind=eng)()


def init_db(engine=None) -> None:
    """Create all tables. Safe to call multiple times."""
    eng = engine or get_engine()
    Base.metadata.create_all(eng)
    _seed_dim_status(eng)
    print("✅  Star Schema tables created in PostgreSQL")


def _seed_dim_status(engine) -> None:
    """Populate dim_status with all AIS navigation status codes."""
    session = sessionmaker(bind=engine)()
    statuses = [
        (0,  "0",  "Underway using engine",          True,  False, False, False, False),
        (1,  "1",  "At anchor",                       False, True,  True,  False, False),
        (2,  "2",  "Not under command",               False, False, False, False, False),
        (3,  "3",  "Restricted manoeuvrability",      True,  False, False, False, False),
        (5,  "5",  "Moored",                          False, True,  False, True,  False),
        (6,  "6",  "Aground",                         False, True,  False, False, True),
        (7,  "7",  "Engaged in fishing",              True,  False, False, False, False),
        (8,  "8",  "Underway sailing",                True,  False, False, False, False),
        (15, "15", "Not defined",                     False, False, False, False, False),
    ]
    for s_id, code, label, u, st, an, mo, ag in statuses:
        existing = session.query(DimStatus).filter_by(
            status_code=code).first()
        if not existing:
            session.add(DimStatus(
                status_id=s_id, status_code=code,
                status_label=label, is_underway=u,
                is_stopped=st, is_anchored=an,
                is_moored=mo, is_aground=ag,
            ))
    session.commit()
    session.close()


if __name__ == "__main__":
    init_db()
    print("Database initialised with Star Schema ✅")
