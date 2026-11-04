"""Local Olist training stages used by the Airflow DAG."""
from __future__ import annotations

import csv
import hashlib
import os
from pathlib import Path

import numpy as np
import pandas as pd

from olist_model import NUMERIC_COLUMNS, fit_model, save_predictor, split_by_time

REQUIRED_COLUMNS = (
    "order_id", "order_purchase_timestamp", "price", "freight", "weight",
    "n_items", "installments", "est_days", "customer_state", "seller_state",
    "category", "payment_type", "late",
)


def _workdir() -> Path:
    return Path(os.environ.get("OLIST_WORKDIR", "/opt/olist/work")).resolve()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def extract(run_key: str) -> str:
    """Export one consistent database snapshot to a run-specific UTF-8 CSV."""
    import psycopg

    if not run_key:
        raise ValueError("run_key must not be empty")
    database_url = os.environ["DATABASE_URL"].replace("postgresql+psycopg://", "postgresql://", 1)
    run_dir = _workdir() / hashlib.sha256(run_key.encode("utf-8")).hexdigest()[:24]
    run_dir.mkdir(parents=True, exist_ok=True)
    snapshot = run_dir / "snapshot.csv"
    temporary = run_dir / "snapshot.csv.tmp"
    try:
        with psycopg.connect(database_url) as connection:
            with connection.cursor(name="olist_snapshot") as cursor:
                cursor.execute("SELECT * FROM public.orders ORDER BY order_purchase_timestamp, order_id")
                columns = [column.name for column in cursor.description]
                with temporary.open("w", encoding="utf-8", newline="") as stream:
                    writer = csv.writer(stream)
                    writer.writerow(columns)
                    for rows in iter(lambda: cursor.fetchmany(1000), []):
                        writer.writerows(rows)
        temporary.replace(snapshot)
    finally:
        temporary.unlink(missing_ok=True)
    return str(snapshot.resolve())


def validate(snapshot: str) -> str:
    """Read and check the CSV without modifying files or external state."""
    path = Path(snapshot)
    with path.open("r", encoding="utf-8", newline="") as stream:
        header = next(csv.reader(stream), None)
    if header is None or len(header) != len(set(header)):
        raise ValueError("CSV is empty or contains duplicate column names")
    missing = set(REQUIRED_COLUMNS) - set(header)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")
    frame = pd.read_csv(path)
    if frame.empty:
        raise ValueError("Snapshot has no orders")
    if frame["order_id"].isna().any() or frame["order_id"].astype(str).str.strip().eq("").any():
        raise ValueError("order_id must be present for every row")
    if not frame["order_id"].is_unique:
        raise ValueError("order_id must be unique")
    try:
        numbers = frame[NUMERIC_COLUMNS].apply(pd.to_numeric, errors="raise").to_numpy(dtype=np.float64)
        labels = pd.to_numeric(frame["late"], errors="raise").to_numpy(dtype=np.float64)
    except (TypeError, ValueError) as exc:
        raise ValueError("Numeric features and late must contain numbers") from exc
    if not np.isfinite(numbers).all():
        raise ValueError("Numeric features must be finite")
    if not np.isfinite(labels).all() or not np.isin(labels, [0, 1]).all():
        raise ValueError("late must be 0 or 1")
    dates = pd.to_datetime(frame["order_purchase_timestamp"], format="ISO8601", errors="raise", utc=True)
    if dates.isna().any():
        raise ValueError("Purchase timestamps must be present")
    return str(path.resolve())


def prepare(snapshot: str) -> dict[str, str | int]:
    """Split chronologically and retain provenance of the original CSV."""
    path = Path(snapshot).resolve()
    checksum = _sha256(path)
    cutoff = os.environ.get("OLIST_CUTOFF", "2017-07-31")
    frame = pd.read_csv(path)
    train_frame, validation_frame = split_by_time(frame, cutoff=cutoff)
    train_path = path.parent / "train.csv"
    validation_path = path.parent / "validation.csv"
    train_frame.to_csv(train_path, index=False, encoding="utf-8")
    validation_frame.to_csv(validation_path, index=False, encoding="utf-8")
    return {
        "snapshot_path": str(path),
        "snapshot_sha256": checksum,
        "train_path": str(train_path),
        "validation_path": str(validation_path),
        "cutoff": cutoff,
        "train_rows": len(train_frame),
        "validation_rows": len(validation_frame),
    }


def train(prepared: dict, *, dag_run_id: str, epochs: int | None = None, seed: int | None = None) -> str:
    """Train the supplied model and record a reloadable MLflow artifact."""
    import mlflow

    epochs = int(os.environ.get("OLIST_EPOCHS", "5")) if epochs is None else int(epochs)
    seed = int(os.environ.get("OLIST_SEED", "42")) if seed is None else int(seed)
    lr = 0.003
    train_frame = pd.read_csv(prepared["train_path"])
    validation_frame = pd.read_csv(prepared["validation_path"])
    if len(train_frame) != prepared["train_rows"] or len(validation_frame) != prepared["validation_rows"]:
        raise ValueError("Prepared row counts changed")
    if _sha256(Path(prepared["snapshot_path"])) != prepared["snapshot_sha256"]:
        raise ValueError("Original snapshot changed after preparation")
    predictor, history = fit_model(train_frame, validation_frame, epochs=epochs, seed=seed, lr=lr)
    model_dir = Path(prepared["train_path"]).parent / "model"
    save_predictor(predictor, model_dir)

    mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
    mlflow.set_experiment("olist_training")
    with mlflow.start_run() as run:
        mlflow.set_tag("dag_run_id", dag_run_id)
        mlflow.log_params({
            "epochs": epochs, "seed": seed, "lr": lr, "cutoff": prepared["cutoff"],
            "train_rows": prepared["train_rows"],
            "validation_rows": prepared["validation_rows"],
            "snapshot_sha256": prepared["snapshot_sha256"],
        })
        for item in history:
            mlflow.log_metric("train_loss", item["train_loss"], step=item["epoch"])
            mlflow.log_metric("validation_pr_auc", item["validation_pr_auc"], step=item["epoch"])
        mlflow.log_artifacts(str(model_dir), artifact_path="model")
        return run.info.run_id
