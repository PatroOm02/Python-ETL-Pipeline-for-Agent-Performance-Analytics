import argparse
import logging
import sys
import os
from pathlib import Path
import warnings

import pandas as pd
import requests

# Suppress pandas date parsing warnings
warnings.filterwarnings(
    "ignore",
    message="Parsing dates in.*dayfirst=True.*"
)

# ─────────────────────────────────────────────────────
# Required columns for each input
# ─────────────────────────────────────────────────────
REQ_CALL_COLS = {
    "call_id", "agent_id", "org_id", "installment_id",
    "status", "duration", "created_ts", "call_date",
}
REQ_AGENT_COLS = {
    "agent_id", "users_first_name", "users_last_name",
    "users_office_location", "org_id",
}
REQ_DISP_COLS = {"agent_id", "org_id", "call_date", "login_time"}


def read_and_validate(path: Path, required: set, name: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    missing = required - set(df.columns)
    if missing:
        logging.error("%s → missing columns: %s", name, ", ".join(missing))
        raise ValueError(f"{name}: missing columns {missing}")

    df.columns = [c.strip() for c in df.columns]
    df["agent_id"] = df["agent_id"].astype(str).str.strip()
    df["org_id"] = df["org_id"].astype(str).str.strip()

    # Parse call_date using explicit formats
    if "call_date" in df.columns:
        if name == "Call Logs":
            df["call_date"] = pd.to_datetime(
                df["call_date"].astype(str).str.replace("\u2011", "-"),
                format="%d-%m-%Y", errors="coerce"
            )
        else:
            df["call_date"] = pd.to_datetime(
                df["call_date"].astype(str),
                format="%Y-%m-%d", errors="coerce"
            )

    # Drop exact duplicates
    dups = df.duplicated()
    if dups.any():
        dup_count = dups.sum()
        logging.warning(
            "%s → %d duplicate rows found, keeping first occurrence", name, dup_count
        )
        df = df[~dups]

    return df


def merge_frames(calls: pd.DataFrame, agents: pd.DataFrame, disp: pd.DataFrame) -> pd.DataFrame:
    # Convert call_date to string for merging
    calls["call_date"] = calls["call_date"].dt.strftime('%Y-%m-%d')
    disp["call_date"] = disp["call_date"].dt.strftime('%Y-%m-%d')

    merged = calls.merge(agents, on=["agent_id", "org_id"], how="left")
    missing_meta = merged["users_first_name"].isna().sum()
    if missing_meta:
        logging.info("%d calls missing agent metadata", missing_meta)

    merged = merged.merge(
        disp, on=["agent_id", "org_id", "call_date"], how="left"
    )
    merged["presence"] = merged["login_time"].notna().astype(int)
    return merged


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    success_values = ["completed", "connected"]
    agg = (
        df.groupby(["call_date", "agent_id"], dropna=False)
          .agg(
              users_first_name=("users_first_name", "first"),
              users_last_name=("users_last_name", "first"),
              users_office_location=("users_office_location", "first"),
              org_id=("org_id", "first"),
              total_calls=("call_id", "count"),
              unique_loans=("installment_id", pd.Series.nunique),
              successful_calls=(
                  "status", lambda s: s.str.lower().isin(success_values).sum()
              ),
              avg_duration_min=("duration", "mean"),
              presence=("presence", "max"),
          )
          .reset_index()
    )
    agg["connect_rate"] = (agg["successful_calls"] / agg["total_calls"]).round(3)
    agg["avg_duration_min"] = agg["avg_duration_min"].round(2)
    return agg


def slack_message(summary: pd.DataFrame, report_date: str) -> str:
    header = f"Agent Summary for {report_date}"
    if summary.empty:
        return header + "\nNo calls were logged."
    top = summary.loc[summary["connect_rate"].idxmax()]
    name = f"{top.users_first_name} {top.users_last_name}"
    rate = f"{top.connect_rate:.0%}"
    msg = (
        f"{header}\n"
        f"*Top Performer*: {name} ({rate} connect rate)\n"
        f"*Total Active Agents*: {summary['agent_id'].nunique()}\n"
        f"*Average Duration*: {summary['avg_duration_min'].mean():.1f} min"
    )
    return msg


def main():
    parser = argparse.ArgumentParser(
        description="Build agent performance summary."
    )
    parser.add_argument(
        "--call_logs", required=True, type=Path,
        help="Path to call_logs.csv"
    )
    parser.add_argument(
        "--agent_roster", required=True, type=Path,
        help="Path to agent_roster.csv"
    )
    parser.add_argument(
        "--disposition", required=True, type=Path,
        help="Path to disposition_summary.csv"
    )
    parser.add_argument(
        "--out", default=Path("agent_performance_summary.csv"), type=Path,
        help="Output CSV file"
    )
    parser.add_argument(
        "--log_level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level"
    )
    parser.add_argument(
        "--slack_webhook", type=str,
        help="Slack webhook URL (overrides SLACK_WEBHOOK_URL env)"
    )
    args = parser.parse_args()
    os.makedirs("logs", exist_ok=True)

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s | %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("logs/pipeline.log", mode="a", encoding="utf-8")
        ]
    )

    # Read + validate
    calls = read_and_validate(args.call_logs, REQ_CALL_COLS, "Call Logs")
    roster = read_and_validate(args.agent_roster, REQ_AGENT_COLS, "Agent Roster")
    disp = read_and_validate(args.disposition, REQ_DISP_COLS, "Disposition Summary")

    # Determine report date safely
    max_date = calls["call_date"].max()
    report_date = (pd.Timestamp.now().strftime("%Y-%m-%d")
                   if pd.isna(max_date) else max_date.strftime("%Y-%m-%d"))

    # Warn on non-positive durations
    invalid = (calls["duration"] <= 0)
    if invalid.any():
        logging.warning("%d rows have non-positive duration", invalid.sum())

    # Merge & feature engineering
    merged = merge_frames(calls, roster, disp)
    summary = engineer_features(merged)

    # Write output
    summary.to_csv(args.out, index=False)
    logging.info("Report written to %s (%d rows)", args.out, len(summary))

    # Slack notification
    webhook = args.slack_webhook or os.getenv("SLACK_WEBHOOK_URL")
    msg = slack_message(summary, report_date)
    if webhook:
        try:
            resp = requests.post(webhook, json={"text": msg})
            text = (resp.text or "").lower()
            if not resp.ok or "no_team" in text:
                logging.warning("Slack notification issue: %s", resp.text)
        except requests.RequestException as e:
            logging.error("Error sending Slack notification: %s", e)
    else:
        logging.info("Slack webhook not provided; skipping notification")

    # Print to console
    print("\n" + msg + "\n")


if __name__ == "__main__":
    main()

