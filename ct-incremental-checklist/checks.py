import asyncio
import base64
import json
import os
import re
from datetime import datetime, timezone

import aioboto3
import httpx

CT_BUCKET = "com.uberresearch.data.clinicaltrials"
BULK_BUCKET = "ai.dimensions.data"
SOLR_BUCKET = "ai-dimensions-data-deliveries-solr"
STALE_DAYS = 3

SOLR_CORE_URL = "https://solr.solr-all-prod2.dimensions.ai/solr/clinical_trials/query"
SOLR_CORE_AUTH = "Basic " + base64.b64encode(b"solr_qa:wcj9nwe!JFB-xzr2yeq").decode()

AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

MONTH_INDEX = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
}


# ── low-level helpers ─────────────────────────────────────────────────────────

async def list_sub_folders(s3, bucket, prefix):
    folders = []
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix, "Delimiter": "/"}
        if token:
            kwargs["ContinuationToken"] = token
        res = await s3.list_objects_v2(**kwargs)
        for p in res.get("CommonPrefixes", []) or []:
            folders.append(p["Prefix"])
        if not res.get("IsTruncated"):
            break
        token = res.get("NextContinuationToken")
    return folders


async def list_objects(s3, bucket, prefix):
    objects = []
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        res = await s3.list_objects_v2(**kwargs)
        for o in res.get("Contents", []) or []:
            objects.append(o)
        if not res.get("IsTruncated"):
            break
        token = res.get("NextContinuationToken")
    return objects


async def file_exists(s3, bucket, key):
    try:
        await s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


def folder_name(prefix):
    return prefix.rstrip("/").split("/")[-1]


def age_days_from_now(dt):
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - dt).total_seconds() / 86400.0


def week_label(days):
    return "more than 1 week" if days > 7 else "less than 1 week"


# ── date parsing ──────────────────────────────────────────────────────────────

def parse_date_ymd(name):
    m = re.match(r"^(\d{4})(\d{2})(\d{2})$", name)
    if not m:
        return None
    return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)), tzinfo=timezone.utc)


def parse_date_range(name):
    m = re.match(r"^(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})$", name)
    if not m:
        return None
    start = datetime.strptime(m.group(1), "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = datetime.strptime(m.group(2), "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return {"start": start, "end": end}


async def newest_sub_folder(s3, bucket, prefix):
    """Find the newest sub-folder under prefix.

    Tries YYYYMMDD name sort, then YYYY-MM-DD_YYYY-MM-DD range sort,
    then falls back to scanning object LastModified.
    """
    folders = await list_sub_folders(s3, bucket, prefix)
    if not folders:
        return None

    dated = []
    for f in folders:
        d = parse_date_ymd(folder_name(f))
        if d:
            dated.append({"prefix": f, "name": folder_name(f), "date": d})
    if dated:
        dated.sort(key=lambda x: x["date"], reverse=True)
        n = dated[0]
        return {**n, "ageDays": int(age_days_from_now(n["date"]))}

    ranged = []
    for f in folders:
        r = parse_date_range(folder_name(f))
        if r:
            ranged.append({"prefix": f, "name": folder_name(f), "range": r})
    if ranged:
        ranged.sort(key=lambda x: x["range"]["end"], reverse=True)
        n = ranged[0]
        return {**n, "ageDays": int(age_days_from_now(n["range"]["end"]))}

    async def get_ts(f):
        try:
            res = await s3.list_objects_v2(Bucket=bucket, Prefix=f, MaxKeys=50)
            times = [o["LastModified"] for o in res.get("Contents", []) or []]
            ts = max(times) if times else None
            return {"prefix": f, "name": folder_name(f), "ts": ts}
        except Exception:
            return {"prefix": f, "name": folder_name(f), "ts": None}

    with_times = await asyncio.gather(*(get_ts(f) for f in folders))
    with_times = [w for w in with_times if w["ts"] is not None]
    if not with_times:
        return None
    with_times.sort(key=lambda x: x["ts"], reverse=True)
    best = with_times[0]
    return {"prefix": best["prefix"], "name": best["name"], "ageDays": int(age_days_from_now(best["ts"]))}


async def check_folder_contents(s3, bucket, prefix, stale_days=STALE_DAYS):
    """Find the latest sub-folder under prefix (by lexicographic name sort) and verify:
      - contains artifact-v2.json
      - contains at least one .jsonl.gz
      - all .jsonl.gz files are not older than stale_days
    """
    folders = await list_sub_folders(s3, bucket, prefix)
    if not folders:
        return {"ok": False, "folder": None, "detail": "no sub-folders found"}

    sorted_folders = sorted(folders)
    latest_prefix = sorted_folders[-1]
    name = folder_name(latest_prefix)

    objects = await list_objects(s3, bucket, latest_prefix)
    has_artifact = any(o["Key"].endswith("artifact-v2.json") for o in objects)
    jsonl_files = [o for o in objects if o["Key"].endswith(".jsonl.gz")]
    stale_jsonls = [o for o in jsonl_files if age_days_from_now(o["LastModified"]) > stale_days]

    issues = []
    if not has_artifact:
        issues.append("missing artifact-v2.json")
    if not jsonl_files:
        issues.append("no .jsonl.gz files")
    if stale_jsonls:
        issues.append(f"{len(stale_jsonls)} .jsonl.gz stale")

    ok = not issues
    return {
        "ok": ok,
        "folder": name,
        "detail": (
            f"{name}  ({len(jsonl_files)} jsonl.gz, fresh)"
            if ok else f"{name}  — {', '.join(issues)}"
        ),
    }


# ── ICTRP filename parsing ────────────────────────────────────────────────────

def parse_ictrp_date(filename):
    # e.g. ICTRPWeek13April2026.zip → 2026-04-13
    m = re.match(r"ICTRPWeek(\d{1,2})([A-Za-z]+)(\d{4})\.zip$", filename, re.IGNORECASE)
    if not m:
        return None
    month = MONTH_INDEX.get(m.group(2).lower())
    if month is None:
        return None
    return datetime(int(m.group(3)), month, int(m.group(1)), tzinfo=timezone.utc)


# ── individual checks ─────────────────────────────────────────────────────────

async def check1_crawlers_freshness(s3, version):
    label = "Crawlers have crawled data"
    crawler_folders = await list_sub_folders(s3, CT_BUCKET, f"{version}/")
    if not crawler_folders:
        return {"ok": False, "label": label, "details": [f"no crawler folders found under {version}/"]}

    async def per_crawler(cp):
        name = folder_name(cp)
        try:
            newest = await newest_sub_folder(s3, CT_BUCKET, cp)
            if not newest:
                return {"name": name, "ok": False, "detail": "no date folders found"}
            stale = newest["ageDays"] > STALE_DAYS
            completed = await file_exists(s3, CT_BUCKET, f"{newest['prefix']}__COMPLETED__")
            ok = (not stale) and completed
            detail = f"latest: {newest['name']} ({week_label(newest['ageDays'])} ago)"
            if stale:
                detail += " — TOO OLD"
            if not completed:
                detail += " — missing __COMPLETED__"
            return {"name": name, "ok": ok, "detail": detail}
        except Exception as e:
            return {"name": name, "ok": False, "detail": str(e)}

    results = await asyncio.gather(*(per_crawler(cp) for cp in crawler_folders))
    failures = [r for r in results if not r["ok"]]
    return {
        "ok": not failures,
        "label": label,
        "details": (
            [f"{len(failures)} crawler(s) not compliant:"]
            + [f"  • {f['name']}: {f['detail']}" for f in failures]
        ) if failures else [],
    }


async def check2_crawlers_mapped(s3, version):
    label = "Crawlers have been mapped"
    crawler_folders, generic_folders = await asyncio.gather(
        list_sub_folders(s3, CT_BUCKET, f"{version}/"),
        list_sub_folders(s3, CT_BUCKET, f"k8s/generic/{version}/"),
    )

    if not crawler_folders:
        return {"ok": False, "label": label, "details": ["no crawler folders found"]}

    async def get_newest_range(prefix):
        try:
            subs = await list_sub_folders(s3, CT_BUCKET, prefix)
            ranged = []
            for f in subs:
                r = parse_date_range(folder_name(f))
                if r:
                    ranged.append({"name": folder_name(f), "range": r})
            if not ranged:
                return None
            ranged.sort(key=lambda x: x["range"]["end"], reverse=True)
            return ranged[0]
        except Exception:
            return None

    async def resolve(prefix):
        return folder_name(prefix), await get_newest_range(prefix)

    crawler_pairs, generic_pairs = await asyncio.gather(
        asyncio.gather(*(resolve(cp) for cp in crawler_folders)),
        asyncio.gather(*(resolve(gp) for gp in generic_folders)),
    )

    crawler_by_name = dict(crawler_pairs)
    generic_by_name = dict(generic_pairs)

    results = []
    for name, crawler_newest in crawler_by_name.items():
        if not crawler_newest:
            results.append({"name": name, "ok": False, "detail": "no date-range folder found in crawler"})
            continue
        generic_newest = generic_by_name.get(name)
        if not generic_newest:
            results.append({"name": name, "ok": False, "detail": "no generic mapping found"})
            continue
        diff_days = (generic_newest["range"]["start"] - crawler_newest["range"]["end"]).total_seconds() / 86400.0
        ok = diff_days <= STALE_DAYS
        if ok:
            detail = f"crawler: {crawler_newest['name']}, generic: {generic_newest['name']} (diff: {week_label(diff_days)})"
        else:
            detail = (
                f"crawler: {crawler_newest['name']}, generic: {generic_newest['name']} "
                f"— diff out of range ({week_label(diff_days)})"
            )
        results.append({"name": name, "ok": ok, "detail": detail})

    failures = [r for r in results if not r["ok"]]
    return {
        "ok": not failures,
        "label": label,
        "details": (
            [f"{len(failures)} mapping(s) not compliant:"]
            + [f"  • {f['name']}: {f['detail']}" for f in failures]
        ) if failures else [],
    }


async def check3_bulk_export(s3, date_release_build):
    label = "Bulk export (client facing) up to date"
    try:
        r = await check_folder_contents(s3, BULK_BUCKET, f"clinicaltrials/{date_release_build}/")
        return {"ok": r["ok"], "label": label, "details": [] if r["ok"] else [r["detail"]]}
    except Exception as e:
        return {"ok": False, "label": label, "details": [f"error: {e}"]}


async def check4_solr_enrichments(s3, date_release_build):
    label = "Solr data bucket from pipeline enrichments up to date"
    try:
        r = await check_folder_contents(s3, CT_BUCKET, f"k8s/solr_export/{date_release_build}/")
        return {"ok": r["ok"], "label": label, "details": [] if r["ok"] else [r["detail"]]}
    except Exception as e:
        return {"ok": False, "label": label, "details": [f"error: {e}"]}


async def check5_solr_sync_prod(s3, version):
    label = "Solr data bucket sync prod folder up to date"
    version_u = version.replace(".", "_")
    try:
        r = await check_folder_contents(s3, SOLR_BUCKET, f"data/clinical_trials/{version_u}/")
        return {"ok": r["ok"], "label": label, "details": [] if r["ok"] else [r["detail"]]}
    except Exception as e:
        return {"ok": False, "label": label, "details": [f"error: {e}"]}


async def check6_pipeline_input(s3, date_release_build):
    label = "Pipeline input (updates from crawlers) up to date"
    try:
        r = await check_folder_contents(s3, CT_BUCKET, f"k8s/pipeline-input/{date_release_build}/")
        return {"ok": r["ok"], "label": label, "details": [] if r["ok"] else [r["detail"]]}
    except Exception as e:
        return {"ok": False, "label": label, "details": [f"error: {e}"]}


async def check7_gbq_export(s3, date_release_build):
    label = "GBQ export file up to date"
    try:
        r = await check_folder_contents(s3, CT_BUCKET, f"k8s/gbq_export/{date_release_build}/")
        return {"ok": r["ok"], "label": label, "details": [] if r["ok"] else [r["detail"]]}
    except Exception as e:
        return {"ok": False, "label": label, "details": [f"error: {e}"]}


async def check8_ictrp_zips(s3):
    label = "ICTRP zip files up to date"
    try:
        year = str(datetime.now(timezone.utc).year)
        year_folders = await list_sub_folders(s3, CT_BUCKET, f"ictrp/{year}/")
        if not year_folders:
            return {"ok": False, "label": label, "details": [f"no folders found under ictrp/{year}/"]}

        latest_folder = sorted(year_folders)[-1]
        objects = await list_objects(s3, CT_BUCKET, latest_folder)
        zip_files = [o for o in objects if o["Key"].lower().endswith(".zip")]

        if not zip_files:
            return {"ok": False, "label": label, "details": [f"no .zip files in {folder_name(latest_folder)}"]}

        newest_date = None
        newest_name = None
        for obj in zip_files:
            fname = obj["Key"].split("/")[-1]
            d = parse_ictrp_date(fname)
            if d and (newest_date is None or d > newest_date):
                newest_date = d
                newest_name = fname

        if newest_date is None:
            return {"ok": False, "label": label, "details": ["cannot parse dates from zip filenames"]}

        age_days = int(age_days_from_now(newest_date))
        ok = age_days <= 30
        return {
            "ok": ok,
            "label": label,
            "details": [] if ok else [f"latest: {newest_name} ({week_label(age_days)} ago) — TOO OLD"],
        }
    except Exception as e:
        return {"ok": False, "label": label, "details": [f"error: {e}"]}


async def check9_pipeline_sanity(s3, date_release_build):
    label = "Pipeline sanity checks (runs match pipeline-input)"
    try:
        input_prefix = f"k8s/pipeline-input/{date_release_build}/"
        input_folders = await list_sub_folders(s3, CT_BUCKET, input_prefix)
        input_names = {folder_name(f) for f in input_folders}
        if not input_names:
            return {"ok": False, "label": label, "details": [f"no update folders found under {input_prefix}"]}

        runs_prefix = f"k8s/pipeline-runs/{date_release_build}/"
        step_folders = await list_sub_folders(s3, CT_BUCKET, runs_prefix)
        if not step_folders:
            return {"ok": False, "label": label, "details": ["no pipeline-runs step folders found"]}

        async def per_step(sp):
            step_name = folder_name(sp)
            step_subs = await list_sub_folders(s3, CT_BUCKET, sp)
            step_names = {folder_name(f) for f in step_subs}
            missing = sorted(input_names - step_names)
            extra = sorted(step_names - input_names)
            return {"stepName": step_name, "ok": not missing and not extra, "missing": missing, "extra": extra}

        step_results = await asyncio.gather(*(per_step(sp) for sp in step_folders))
        failures = [r for r in step_results if not r["ok"]]
        if not failures:
            return {"ok": True, "label": label, "details": []}

        details = [f"{len(failures)} step(s) with discrepancies:"]
        groups = {}
        for f in failures:
            key = json.dumps({"missing": sorted(f["missing"]), "extra": sorted(f["extra"])})
            if key not in groups:
                groups[key] = {"missing": f["missing"], "extra": f["extra"], "steps": []}
            groups[key]["steps"].append(f["stepName"])

        for g in groups.values():
            steps = g["steps"]
            steps_label = steps[0] if len(steps) == 1 else f"{len(steps)} steps ({', '.join(steps)})"
            details.append(f"  • {steps_label}:")
            if g["missing"]:
                details.append(f"    missing updates: {', '.join(g['missing'])}")
            if g["extra"]:
                details.append(f"    extra updates:   {', '.join(g['extra'])}")
        return {"ok": False, "label": label, "details": details}
    except Exception as e:
        return {"ok": False, "label": label, "details": [f"error: {e}"]}


async def check10_solr_core():
    label = "Solr core in production up to date"
    try:
        json_facet = json.dumps({
            "by_registry": {
                "type": "terms", "field": "registry", "limit": -1,
                "facet": {"last_created": "max(created_in_dimensions)"},
            }
        })
        params = {
            "q": "*:*", "q.op": "OR", "rows": "0",
            "fl": "id,created_in_dimensions",
            "facet": "true", "facet.field": "registry", "facet.limit": "-1",
            "json.facet": json_facet,
        }

        async with httpx.AsyncClient(timeout=60.0) as client:
            res = await client.get(SOLR_CORE_URL, params=params, headers={"Authorization": SOLR_CORE_AUTH})

        if res.status_code != 200:
            return {"ok": False, "label": label, "details": [f"HTTP {res.status_code}"]}

        data = res.json()
        buckets = (((data or {}).get("facets") or {}).get("by_registry") or {}).get("buckets") or []
        if not buckets:
            return {"ok": False, "label": label, "details": ["no registry data in response"]}

        stale_registries = []
        global_max = None

        for b in buckets:
            last_created_str = b["last_created"]
            ts = datetime.fromisoformat(last_created_str.replace("Z", "+00:00"))
            if global_max is None or ts > global_max:
                global_max = ts
            age_days = age_days_from_now(ts)
            if age_days > 6:
                stale_registries.append({
                    "val": b["val"],
                    "ageDays": int(age_days),
                    "date": last_created_str[:10],
                })

        global_age_days = age_days_from_now(global_max)
        global_ok = global_age_days <= 7
        per_reg_ok = not stale_registries
        ok = global_ok and per_reg_ok

        if ok:
            return {"ok": True, "label": label, "details": []}

        details = []
        if not global_ok:
            details.append(
                f"Overall: most recent created_in_dimensions is "
                f"{global_max.strftime('%Y-%m-%d')} ({week_label(global_age_days)} ago)"
            )
        if not per_reg_ok:
            details.append("Stale registries:")
            for r in stale_registries:
                details.append(f"  - {r['val']}: {r['date']} ({week_label(r['ageDays'])} ago)")
        return {"ok": False, "label": label, "details": details}
    except Exception as e:
        return {"ok": False, "label": label, "details": [f"error: {e}"]}


# ── main checklist ────────────────────────────────────────────────────────────

async def run_checklist(version, date_release_build):
    session = aioboto3.Session()
    async with session.client("s3", region_name=AWS_REGION) as s3:
        checks = await asyncio.gather(
            check1_crawlers_freshness(s3, version),
            check2_crawlers_mapped(s3, version),
            check3_bulk_export(s3, date_release_build),
            check4_solr_enrichments(s3, date_release_build),
            check5_solr_sync_prod(s3, version),
            check6_pipeline_input(s3, date_release_build),
            check7_gbq_export(s3, date_release_build),
            check8_ictrp_zips(s3),
            check9_pipeline_sanity(s3, date_release_build),
            check10_solr_core(),
        )

    all_ok = all(c["ok"] for c in checks)

    summary_lines = [
        f"{str(i + 1).rjust(2)}. {'✅' if c['ok'] else '❌'}  {c['label']}"
        for i, c in enumerate(checks)
    ]

    detail_lines = []
    for i, c in enumerate(checks):
        if c["ok"] or not c["details"]:
            continue
        detail_lines.append("")
        detail_lines.append(f"{str(i + 1).rjust(2)}. ❌  {c['label']}:")
        for d in c["details"]:
            detail_lines.append(f"    {d}")

    lines = [
        "╔══════════════════════════════════════════════════════════════╗",
        "  🔄  ClinicalTrials Incremental Updates Checklist",
        f"       version: {version}   build: {date_release_build}",
        "╚══════════════════════════════════════════════════════════════╝",
        "",
        *summary_lines,
    ]
    if detail_lines:
        lines += ["", f"── Details {'─' * 49}", *detail_lines]
    lines += [
        "",
        "───────────────────────────────────────────────────────────────",
        (
            f"✅  All checks passed — incremental updates for {version} look good!"
            if all_ok else "❌  Some checks failed — review the details above."
        ),
    ]

    return lines, all_ok
