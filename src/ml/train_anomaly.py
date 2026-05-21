"""
train_anomaly.py — Maritime Navigation AI System
Trains Isolation Forest anomaly detector on Silver TRAIN split.
Saves model to models/ folder for use by live scorer.

Run after silver_job.py:
    docker compose exec producer python src/ml/train_anomaly.py
"""
import os, sys, joblib
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

sys.path.insert(0, "/app/src/common")
from config import (
    DELTA_SILVER_PATH, MODELS_PATH,
    ANOMALY_CONTAMINATION,
)

FEATURES = [
    "sog", "cog", "heading",
    "lat", "lon",
    "sog_change", "heading_change",
    "time_delta_sec", "distance_nm",
    "length", "width", "draft",
]

RULES_CONFIG = {
    "sudden_stop_threshold":   5.0,   # sog drops from > this
    "sharp_turn_threshold":    45.0,  # heading change degrees
    "speed_max_threshold":     30.0,  # kn
    "position_jump_threshold": 3.0,   # ratio actual/expected distance
}

def load_train_data() -> pd.DataFrame:
    """
    Load train split from Parquet using chunked sampling.
    Loads max 2 million rows to avoid memory crash.
    """
    parquet_path = Path("/app/data/parquet")
    silver_path  = Path(DELTA_SILVER_PATH)

    load_path = silver_path if silver_path.exists() else parquet_path

    print(f"📂  Loading from: {load_path}")

    # Get list of all parquet files
    if load_path.is_dir():
        all_files = sorted(load_path.rglob("*.parquet"))
    else:
        all_files = [load_path]

    print(f"    Found {len(all_files)} parquet files")

    # Load files one by one, keep only train split
    # Stop when we have enough rows
    MAX_ROWS   = 2_000_000
    SAMPLE_PER_FILE = MAX_ROWS // max(len(all_files), 1)

    chunks = []
    total  = 0

    for f in all_files:
        try:
            chunk = pd.read_parquet(f)

            # Filter train split
            if "data_split" in chunk.columns:
                chunk = chunk[chunk["data_split"] == "train"]

            if len(chunk) == 0:
                continue

            # Sample to avoid memory issues
            if len(chunk) > SAMPLE_PER_FILE:
                chunk = chunk.sample(
                    n=SAMPLE_PER_FILE, random_state=42
                )

            chunks.append(chunk)
            total += len(chunk)
            print(f"    Loaded {total:,} rows so far...")

            if total >= MAX_ROWS:
                break

        except Exception as e:
            print(f"    Skipping {f.name}: {e}")
            continue

    if not chunks:
        raise FileNotFoundError("No train data found in parquet files")

    df = pd.concat(chunks, ignore_index=True)
    print(f"✅  Train data loaded: {len(df):,} rows, "
          f"{df['mmsi'].nunique():,} vessels")
    return df

def apply_rule_based_labels(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply rule-based anomaly labels to training data.
    These become the ground truth for evaluation.
    """
    df = df.copy()
    df["rule_anomaly"] = False
    df["rule_type"]    = ""

    # Rule 1: Sudden stop
    if "sog_change" in df.columns:
        mask = (
            df["sog_change"].fillna(0) < -RULES_CONFIG["sudden_stop_threshold"]
        )
        df.loc[mask, "rule_anomaly"] = True
        df.loc[mask, "rule_type"]    = "SUDDEN_STOP"

    # Rule 2: Sharp turn at speed
    if "heading_change" in df.columns:
        mask = (
            (df["heading_change"].fillna(0) > RULES_CONFIG["sharp_turn_threshold"]) &
            (df["sog"] > 2.0)
        )
        df.loc[mask, "rule_anomaly"] = True
        df.loc[mask, "rule_type"]    = "SHARP_TURN"

    # Rule 3: Unusual speed
    mask = df["sog"] > RULES_CONFIG["speed_max_threshold"]
    df.loc[mask, "rule_anomaly"] = True
    df.loc[mask, "rule_type"]    = "UNUSUAL_SPEED"

    # Rule 4: Stationary in Suez zone
    if "in_suez_zone" in df.columns:
        mask = (df["sog"] < 0.5) & (df["in_suez_zone"] == True)
        df.loc[mask, "rule_anomaly"] = True
        df.loc[mask, "rule_type"]    = "STATIONARY_RISK"

    anomaly_count = df["rule_anomaly"].sum()
    print(f"    Rule-based anomalies found: {anomaly_count:,} "
          f"({anomaly_count/len(df)*100:.1f}%)")
    
    # Rule 5: AIS signal gap (vessel went silent)
    if "time_delta_sec" in df.columns:
        mask = df["time_delta_sec"] > 300  # silent > 5 minutes
        df.loc[mask, "rule_anomaly"] = True
        df.loc[mask, "rule_type"]    = "AIS_GAP"

    anomaly_count = df["rule_anomaly"].sum()
    return df


def train_isolation_forest(df: pd.DataFrame):
    """Train Isolation Forest on feature matrix."""
    from sklearn.ensemble import IsolationForest
    from sklearn.preprocessing import StandardScaler
    from sklearn.metrics import classification_report

    # Prepare features
    feat_cols = [c for c in FEATURES if c in df.columns]
    X = df[feat_cols].fillna(0).replace([np.inf, -np.inf], 0)

    print(f"\n🔨  Training Isolation Forest")
    print(f"    Samples  : {len(X):,}")
    print(f"    Features : {feat_cols}")
    print(f"    Contamination: {ANOMALY_CONTAMINATION}")

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    model = IsolationForest(
        contamination=ANOMALY_CONTAMINATION,
        n_estimators=200,
        max_samples="auto",
        random_state=42,
        n_jobs=-1,
        verbose=0,
    )
    model.fit(X_scaled)

    # Evaluate against rule-based labels if available
    if "rule_anomaly" in df.columns:
        preds = model.predict(X_scaled)
        # IsolationForest: -1=anomaly, 1=normal
        pred_binary = (preds == -1)
        true_binary = df["rule_anomaly"].values

        print(f"\n📊  Evaluation vs rule-based labels:")
        print(classification_report(
            true_binary, pred_binary,
            target_names=["Normal", "Anomaly"],
            zero_division=0,
        ))

    return model, scaler, feat_cols


def main():
    start = datetime.now()
    print("=" * 55)
    print("  Maritime AI — Anomaly Detector Training")
    print("=" * 55)

    # Load data
    df = load_train_data()

    # Apply rule-based labels for evaluation
    df = apply_rule_based_labels(df)

    # Train model
    model, scaler, features = train_isolation_forest(df)

    # Save
    Path(MODELS_PATH).mkdir(parents=True, exist_ok=True)
    joblib.dump(model,    f"{MODELS_PATH}/isolation_forest.pkl")
    joblib.dump(scaler,   f"{MODELS_PATH}/scaler_anomaly.pkl")
    joblib.dump(features, f"{MODELS_PATH}/anomaly_features.pkl")
    joblib.dump(RULES_CONFIG, f"{MODELS_PATH}/rules_config.pkl")

    elapsed = (datetime.now() - start).seconds
    print(f"\n✅  Models saved to {MODELS_PATH}/")
    print(f"    Training time: {elapsed}s")
    print(f"\n    Next: python src/ml/train_predictor.py")


if __name__ == "__main__":
    main()
