"""
EMR Serverless cost calculator for a time window using CloudWatch GetMetricData.

Sums billable CPU / memory / storage across all (WorkerType, CapacityAllocationType)
combinations at per-minute granularity, then applies regional rates.

Usage:
    python emr_svless_cost.py \
        --app-id *********** \
        --start "2026-05-19 13:21:00" \
        --end   "2026-05-19 14:40:00"
"""

import argparse
import boto3
from datetime import datetime, timedelta, timezone

APP_ID_DEFAULT   = "" # EMR Serverless Application ID, e.g. "00g5prif4c****"
APP_NAME_DEFAULT = "caris-poc-spark-s3tables"
REGION_DEFAULT   = "" # AWS Region

RATES = {
    "ap-southeast-1": {"vcpu": 0.0633, "mem": 0.00696, "storage": 0.000111},
    "us-east-1":      {"vcpu": 0.0520, "mem": 0.00571, "storage": 0.000111},
}

WORKER_TYPES   = ["Spark_Driver", "Spark_Executor", "Spark_Kernel"]
CAPACITY_TYPES = ["OnDemandCapacity", "PreInitCapacity"]
METRICS = {
    "vcpu":    "CPUAllocated",
    "mem":     "MemoryAllocated",
    "storage": "StorageAllocated",
}
PERIOD_SECONDS = 60


def parse_time(s: str) -> datetime:
    dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S") if len(s) > 16 \
         else datetime.strptime(s, "%Y-%m-%d %H:%M")
    return dt.replace(tzinfo=timezone.utc)


def floor_minute(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)


def ceil_minute(dt: datetime) -> datetime:
    if dt.second == 0 and dt.microsecond == 0:
        return dt
    return floor_minute(dt) + timedelta(minutes=1)


def build_queries():
    """One MetricDataQuery per (resource, worker_type, capacity_type) combo."""
    queries = []
    for res_key, metric_name in METRICS.items():
        for wt in WORKER_TYPES:
            for ct in CAPACITY_TYPES:
                qid = f"{res_key}_{wt}_{ct}".lower().replace("capacity", "")
                queries.append({
                    "Id": qid,
                    "Label": f"{res_key}|{wt}|{ct}",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/EMRServerless",
                            "MetricName": metric_name,
                            "Dimensions": [
                                {"Name": "ApplicationId",         "Value": "__APP_ID__"},
                                {"Name": "ApplicationName",       "Value": "__APP_NAME__"},
                                {"Name": "WorkerType",            "Value": wt},
                                {"Name": "CapacityAllocationType","Value": ct},
                            ],
                        },
                        "Period": PERIOD_SECONDS,
                        "Stat": "Average",
                    },
                    "ReturnData": True,
                })
    return queries


def fetch(cw, app_id, app_name, start, end, debug=False):
    queries = build_queries()
    for q in queries:
        for d in q["MetricStat"]["Metric"]["Dimensions"]:
            if d["Value"] == "__APP_ID__":
                d["Value"] = app_id
            elif d["Value"] == "__APP_NAME__":
                d["Value"] = app_name

    # Map Id -> (resource, worker_type, capacity_type) because GetMetricData
    # may not echo our custom Label reliably.
    qid_to_meta = {}
    for res_key in METRICS:
        for wt in WORKER_TYPES:
            for ct in CAPACITY_TYPES:
                qid = f"{res_key}_{wt}_{ct}".lower().replace("capacity", "")
                qid_to_meta[qid] = (res_key, wt, ct)

    results = {}
    paginator = cw.get_paginator("get_metric_data")

    if debug:
        print(f"Querying {len(queries)} metric streams")
        print(f"StartTime={start.isoformat()} EndTime={end.isoformat()}")
        print(f"App ID: {app_id}")
        print()

    for page in paginator.paginate(
        MetricDataQueries=queries,
        StartTime=start,
        EndTime=end,
        ScanBy="TimestampAscending",
    ):
        for r in page["MetricDataResults"]:
            qid = r["Id"]
            res_key, wt, ct = qid_to_meta[qid]
            values = r.get("Values", [])
            status = r.get("StatusCode", "?")

            if debug:
                print(f"  {qid:<55} status={status} points={len(values):<4} "
                      f"sample_values={values[:3]}  label={r.get('Label')!r}")

            unit_hours = sum(v * (PERIOD_SECONDS / 3600) for v in values)
            key = (res_key, wt, ct)
            results[key] = results.get(key, 0.0) + unit_hours

    if debug:
        print()
    return results


def report(totals, region, start, end):
    rates = RATES.get(region)
    if not rates:
        raise SystemExit(f"No rate table for region {region}; add it to RATES.")

    by_resource = {"vcpu": 0.0, "mem": 0.0, "storage": 0.0}
    breakdown = []

    for (res_key, wt, ct), unit_hours in sorted(totals.items()):
        by_resource[res_key] += unit_hours
        if unit_hours > 0:
            breakdown.append((res_key, wt, ct, unit_hours))

    print(f"Window (UTC): {start.strftime('%Y-%m-%d %H:%M')} -> {end.strftime('%Y-%m-%d %H:%M')}")
    print(f"Region: {region}")
    print()
    print(f"{'Resource':<10}{'WorkerType':<18}{'CapacityType':<20}{'Unit-Hours':>14}")
    print("-" * 62)
    for res_key, wt, ct, uh in breakdown:
        print(f"{res_key:<10}{wt:<18}{ct:<20}{uh:>14.4f}")

    print()
    print(f"{'Totals':<28}{'Unit-Hours':>14}{'Rate':>12}{'Cost':>12}")
    print("-" * 66)
    cost_vcpu    = by_resource["vcpu"]    * rates["vcpu"]
    cost_mem     = by_resource["mem"]     * rates["mem"]
    cost_storage = by_resource["storage"] * rates["storage"]
    print(f"{'vCPU-hours':<28}{by_resource['vcpu']:>14.4f}{rates['vcpu']:>12.5f}{cost_vcpu:>12.4f}")
    print(f"{'Memory GB-hours':<28}{by_resource['mem']:>14.4f}{rates['mem']:>12.5f}{cost_mem:>12.4f}")
    print(f"{'Storage GB-hours':<28}{by_resource['storage']:>14.4f}{rates['storage']:>12.6f}{cost_storage:>12.4f}")
    print("-" * 66)
    print(f"{'TOTAL':<54}${cost_vcpu + cost_mem + cost_storage:>10.4f}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--app-id",   default=APP_ID_DEFAULT)
    ap.add_argument("--app-name", default=APP_NAME_DEFAULT)
    ap.add_argument("--region",   default=REGION_DEFAULT)
    ap.add_argument("--start", required=True, help='"YYYY-MM-DD HH:MM" UTC')
    ap.add_argument("--end",   required=True, help='"YYYY-MM-DD HH:MM" UTC')
    ap.add_argument("--debug", action="store_true",
                    help="Print per-stream raw datapoint counts and samples")
    args = ap.parse_args()

    start = floor_minute(parse_time(args.start))
    end   = ceil_minute(parse_time(args.end))

    cw = boto3.client("cloudwatch", region_name=args.region)
    totals = fetch(cw, args.app_id, args.app_name, start, end, debug=args.debug)
    report(totals, args.region, start, end)


if __name__ == "__main__":
    main()
