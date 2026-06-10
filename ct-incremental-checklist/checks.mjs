import {
  S3Client,
  ListObjectsV2Command,
  HeadObjectCommand,
} from "@aws-sdk/client-s3";

const s3 = new S3Client({ region: process.env.AWS_REGION || "us-east-1" });

const CT_BUCKET   = "com.uberresearch.data.clinicaltrials";
const BULK_BUCKET = "ai.dimensions.data";
const SOLR_BUCKET = "ai-dimensions-data-deliveries-solr";
const STALE_DAYS  = 3;

const SOLR_CORE_URL  = "https://solr.solr-all-prod2.dimensions.ai/solr/clinical_trials/query";
const SOLR_CORE_AUTH = "Basic " + Buffer.from("solr_qa:wcj9nwe!JFB-xzr2yeq").toString("base64");

// ── low-level helpers ─────────────────────────────────────────────────────────

async function listSubFolders(bucket, prefix) {
  const folders = [];
  let token;
  do {
    const res = await s3.send(new ListObjectsV2Command({
      Bucket: bucket, Prefix: prefix, Delimiter: "/", ContinuationToken: token,
    }));
    for (const p of res.CommonPrefixes ?? []) folders.push(p.Prefix);
    token = res.IsTruncated ? res.NextContinuationToken : undefined;
  } while (token);
  return folders;
}

async function listObjects(bucket, prefix) {
  const objects = [];
  let token;
  do {
    const res = await s3.send(new ListObjectsV2Command({
      Bucket: bucket, Prefix: prefix, ContinuationToken: token,
    }));
    for (const o of res.Contents ?? []) objects.push(o);
    token = res.IsTruncated ? res.NextContinuationToken : undefined;
  } while (token);
  return objects;
}

async function fileExists(bucket, key) {
  try { await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key })); return true; }
  catch { return false; }
}

function folderName(prefix) {
  return prefix.replace(/\/$/, "").split("/").pop();
}

function ageDaysFromNow(date) {
  return (Date.now() - new Date(date).getTime()) / 86_400_000;
}

function weekLabel(days) {
  return days > 7 ? "more than 1 week" : "less than 1 week";
}

// ── date parsing ──────────────────────────────────────────────────────────────

function parseDateYMD(name) {
  const m = name.match(/^(\d{4})(\d{2})(\d{2})$/);
  return m ? new Date(`${m[1]}-${m[2]}-${m[3]}T00:00:00Z`) : null;
}

/** Parse YYYY-MM-DD_YYYY-MM-DD → { start: Date, end: Date } or null */
function parseDateRange(name) {
  const m = name.match(/^(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})$/);
  if (!m) return null;
  return { start: new Date(m[1] + "T00:00:00Z"), end: new Date(m[2] + "T00:00:00Z") };
}

/**
 * Find the newest sub-folder under prefix.
 * Tries YYYYMMDD name sort, then YYYY-MM-DD_YYYY-MM-DD range sort,
 * then falls back to scanning object LastModified.
 */
async function newestSubFolder(bucket, prefix) {
  const folders = await listSubFolders(bucket, prefix);
  if (!folders.length) return null;

  const dated = folders
    .map(f => ({ prefix: f, name: folderName(f), date: parseDateYMD(folderName(f)) }))
    .filter(f => f.date)
    .sort((a, b) => b.date - a.date);
  if (dated.length) {
    const n = dated[0];
    return { ...n, ageDays: Math.floor(ageDaysFromNow(n.date)) };
  }

  const ranged = folders
    .map(f => { const r = parseDateRange(folderName(f)); return r ? { prefix: f, name: folderName(f), range: r } : null; })
    .filter(Boolean)
    .sort((a, b) => b.range.end - a.range.end);
  if (ranged.length) {
    const n = ranged[0];
    return { ...n, ageDays: Math.floor(ageDaysFromNow(n.range.end)) };
  }

  const withTimes = await Promise.all(
    folders.map(async (f) => {
      try {
        const res = await s3.send(new ListObjectsV2Command({ Bucket: bucket, Prefix: f, MaxKeys: 50 }));
        const ts = Math.max(0, ...(res.Contents ?? []).map(o => new Date(o.LastModified).getTime()));
        return { prefix: f, name: folderName(f), ts };
      } catch {
        return { prefix: f, name: folderName(f), ts: 0 };
      }
    })
  );
  const best = withTimes.sort((a, b) => b.ts - a.ts)[0];
  if (!best?.ts) return null;
  return { prefix: best.prefix, name: best.name, ageDays: Math.floor(ageDaysFromNow(best.ts)) };
}

/**
 * Find the latest sub-folder under prefix (by lexicographic name sort) and verify:
 *   - contains artifact-v2.json
 *   - contains at least one .jsonl.gz
 *   - all .jsonl.gz files are not older than staleDays
 */
async function checkFolderContents(bucket, prefix, staleDays = STALE_DAYS) {
  const folders = await listSubFolders(bucket, prefix);
  if (!folders.length) return { ok: false, folder: null, detail: "no sub-folders found" };

  const sorted      = [...folders].sort();
  const latestPrefix = sorted[sorted.length - 1];
  const name        = folderName(latestPrefix);

  const objects     = await listObjects(bucket, latestPrefix);
  const hasArtifact = objects.some(o => o.Key.endsWith("artifact-v2.json"));
  const jsonlFiles  = objects.filter(o => o.Key.endsWith(".jsonl.gz"));
  const staleJsonls = jsonlFiles.filter(o => ageDaysFromNow(o.LastModified) > staleDays);

  const issues = [];
  if (!hasArtifact)       issues.push("missing artifact-v2.json");
  if (!jsonlFiles.length) issues.push("no .jsonl.gz files");
  if (staleJsonls.length) issues.push(`${staleJsonls.length} .jsonl.gz stale`);

  const ok = !issues.length;
  return {
    ok, folder: name,
    detail: ok
      ? `${name}  (${jsonlFiles.length} jsonl.gz, fresh)`
      : `${name}  — ${issues.join(", ")}`,
  };
}

// ── ICTRP filename parsing ────────────────────────────────────────────────────

const MONTH_INDEX = {
  january: 0, february: 1, march: 2, april: 3, may: 4, june: 5,
  july: 6, august: 7, september: 8, october: 9, november: 10, december: 11,
};

function parseIctrpDate(filename) {
  // e.g. ICTRPWeek13April2026.zip  →  2026-04-13
  const m = filename.match(/ICTRPWeek(\d{1,2})([A-Za-z]+)(\d{4})\.zip$/i);
  if (!m) return null;
  const month = MONTH_INDEX[m[2].toLowerCase()];
  if (month === undefined) return null;
  return new Date(Date.UTC(+m[3], month, +m[1]));
}

// ── individual checks ─────────────────────────────────────────────────────────

async function check1CrawlersFreshness(version) {
  const label = "Crawlers have crawled data";
  const crawlerFolders = await listSubFolders(CT_BUCKET, `${version}/`);
  if (!crawlerFolders.length) {
    return { ok: false, label, details: [`no crawler folders found under ${version}/`] };
  }

  const results = await Promise.all(
    crawlerFolders.map(async (cp) => {
      const name = folderName(cp);
      try {
        const newest = await newestSubFolder(CT_BUCKET, cp);
        if (!newest) return { name, ok: false, detail: "no date folders found" };
        const stale     = newest.ageDays > STALE_DAYS;
        const completed = await fileExists(CT_BUCKET, `${newest.prefix}__COMPLETED__`);
        const ok        = !stale && completed;
        let detail      = `latest: ${newest.name} (${weekLabel(newest.ageDays)} ago)`;
        if (stale)      detail += " — TOO OLD";
        if (!completed) detail += " — missing __COMPLETED__";
        return { name, ok, detail };
      } catch (e) {
        return { name, ok: false, detail: e.message };
      }
    })
  );

  const failures = results.filter(r => !r.ok);
  return {
    ok: !failures.length,
    label,
    details: failures.length
      ? [`${failures.length} crawler(s) not compliant:`, ...failures.map(f => `  • ${f.name}: ${f.detail}`)]
      : [],
  };
}

async function check2CrawlersMapped(version) {
  const label = "Crawlers have been mapped";
  const [crawlerFolders, genericFolders] = await Promise.all([
    listSubFolders(CT_BUCKET, `${version}/`),
    listSubFolders(CT_BUCKET, `k8s/generic/${version}/`),
  ]);

  if (!crawlerFolders.length) {
    return { ok: false, label, details: ["no crawler folders found"] };
  }

  async function getNewestRange(bucket, prefix) {
    const subs = await listSubFolders(bucket, prefix);
    return subs
      .map(f => { const r = parseDateRange(folderName(f)); return r ? { name: folderName(f), range: r } : null; })
      .filter(Boolean)
      .sort((a, b) => b.range.end - a.range.end)[0] ?? null;
  }

  const [crawlerMap, genericMap] = await Promise.all([
    Promise.all(crawlerFolders.map(async (cp) => ({ name: folderName(cp), newest: await getNewestRange(CT_BUCKET, cp).catch(() => null) }))),
    Promise.all(genericFolders.map(async (gp) => ({ name: folderName(gp), newest: await getNewestRange(CT_BUCKET, gp).catch(() => null) }))),
  ]);

  const crawlerByName = Object.fromEntries(crawlerMap.map(r => [r.name, r.newest]));
  const genericByName = Object.fromEntries(genericMap.map(r => [r.name, r.newest]));

  const results = Object.entries(crawlerByName).map(([name, crawlerNewest]) => {
    if (!crawlerNewest) return { name, ok: false, detail: "no date-range folder found in crawler" };
    const genericNewest = genericByName[name];
    if (!genericNewest) return { name, ok: false, detail: "no generic mapping found" };

    const diffDays = (genericNewest.range.start - crawlerNewest.range.end) / 86_400_000;
    const ok = diffDays <= STALE_DAYS;
    return {
      name, ok,
      detail: ok
        ? `crawler: ${crawlerNewest.name}, generic: ${genericNewest.name} (diff: ${weekLabel(diffDays)})`
        : `crawler: ${crawlerNewest.name}, generic: ${genericNewest.name} — diff out of range (${weekLabel(diffDays)})`,
    };
  });

  const failures = results.filter(r => !r.ok);
  return {
    ok: !failures.length,
    label,
    details: failures.length
      ? [`${failures.length} mapping(s) not compliant:`, ...failures.map(f => `  • ${f.name}: ${f.detail}`)]
      : [],
  };
}

async function check3BulkExport(date_release_build) {
  const label = "Bulk export (client facing) up to date";
  try {
    const r = await checkFolderContents(BULK_BUCKET, `clinicaltrials/${date_release_build}/`);
    return { ok: r.ok, label, details: r.ok ? [] : [r.detail] };
  } catch (e) {
    return { ok: false, label, details: [`error: ${e.message}`] };
  }
}

async function check4SolrEnrichments(date_release_build) {
  const label = "Solr data bucket from pipeline enrichments up to date";
  try {
    const r = await checkFolderContents(CT_BUCKET, `k8s/solr_export/${date_release_build}/`);
    return { ok: r.ok, label, details: r.ok ? [] : [r.detail] };
  } catch (e) {
    return { ok: false, label, details: [`error: ${e.message}`] };
  }
}

async function check5SolrSyncProd(version) {
  const label = "Solr data bucket sync prod folder up to date";
  const versionU = version.replaceAll(".", "_");
  try {
    const r = await checkFolderContents(SOLR_BUCKET, `data/clinical_trials/${versionU}/`);
    return { ok: r.ok, label, details: r.ok ? [] : [r.detail] };
  } catch (e) {
    return { ok: false, label, details: [`error: ${e.message}`] };
  }
}

async function check6PipelineInput(date_release_build) {
  const label = "Pipeline input (updates from crawlers) up to date";
  try {
    const r = await checkFolderContents(CT_BUCKET, `k8s/pipeline-input/${date_release_build}/`);
    return { ok: r.ok, label, details: r.ok ? [] : [r.detail] };
  } catch (e) {
    return { ok: false, label, details: [`error: ${e.message}`] };
  }
}

async function check7GbqExport(date_release_build) {
  const label = "GBQ export file up to date";
  try {
    const r = await checkFolderContents(CT_BUCKET, `k8s/gbq_export/${date_release_build}/`);
    return { ok: r.ok, label, details: r.ok ? [] : [r.detail] };
  } catch (e) {
    return { ok: false, label, details: [`error: ${e.message}`] };
  }
}

async function check8IctrpZips() {
  const label = "ICTRP zip files up to date";
  try {
    const year = new Date().getUTCFullYear().toString();
    const yearFolders = await listSubFolders(CT_BUCKET, `ictrp/${year}/`);
    if (!yearFolders.length) {
      return { ok: false, label, details: [`no folders found under ictrp/${year}/`] };
    }

    const latestFolder = [...yearFolders].sort().pop();
    const objects  = await listObjects(CT_BUCKET, latestFolder);
    const zipFiles = objects.filter(o => o.Key.toLowerCase().endsWith(".zip"));

    if (!zipFiles.length) {
      return { ok: false, label, details: [`no .zip files in ${folderName(latestFolder)}`] };
    }

    let newestDate = null;
    let newestName = null;
    for (const obj of zipFiles) {
      const fname = obj.Key.split("/").pop();
      const d = parseIctrpDate(fname);
      if (d && (!newestDate || d > newestDate)) { newestDate = d; newestName = fname; }
    }

    if (!newestDate) {
      return { ok: false, label, details: ["cannot parse dates from zip filenames"] };
    }

    const ageDays = Math.floor(ageDaysFromNow(newestDate));
    const ok = ageDays <= 30;
    return {
      ok, label,
      details: ok ? [] : [`latest: ${newestName} (${weekLabel(ageDays)} ago) — TOO OLD`],
    };
  } catch (e) {
    return { ok: false, label, details: [`error: ${e.message}`] };
  }
}

async function check9PipelineSanity(date_release_build) {
  const label = "Pipeline sanity checks (runs match pipeline-input)";
  try {
    const inputPrefix  = `k8s/pipeline-input/${date_release_build}/`;
    const inputFolders = await listSubFolders(CT_BUCKET, inputPrefix);
    const inputNames   = new Set(inputFolders.map(f => folderName(f)));

    if (!inputNames.size) {
      return { ok: false, label, details: [`no update folders found under ${inputPrefix}`] };
    }

    const runsPrefix  = `k8s/pipeline-runs/${date_release_build}/`;
    const stepFolders = await listSubFolders(CT_BUCKET, runsPrefix);
    if (!stepFolders.length) {
      return { ok: false, label, details: ["no pipeline-runs step folders found"] };
    }

    const stepResults = await Promise.all(
      stepFolders.map(async (sp) => {
        const stepName  = folderName(sp);
        const stepSubs  = await listSubFolders(CT_BUCKET, sp);
        const stepNames = new Set(stepSubs.map(f => folderName(f)));
        const missing   = [...inputNames].filter(n => !stepNames.has(n));
        const extra     = [...stepNames].filter(n => !inputNames.has(n));
        return { stepName, ok: !missing.length && !extra.length, missing, extra };
      })
    );

    const failures = stepResults.filter(r => !r.ok);
    if (!failures.length) return { ok: true, label, details: [] };

    // Group failures that share the same missing/extra sets to avoid repeating 27 identical lines
    const details = [`${failures.length} step(s) with discrepancies:`];
    const groups = new Map();
    for (const f of failures) {
      const key = JSON.stringify({ missing: f.missing.sort(), extra: f.extra.sort() });
      if (!groups.has(key)) groups.set(key, { missing: f.missing, extra: f.extra, steps: [] });
      groups.get(key).steps.push(f.stepName);
    }
    for (const { missing, extra, steps } of groups.values()) {
      const stepsLabel = steps.length === 1 ? steps[0] : `${steps.length} steps (${steps.join(", ")})`;
      details.push(`  • ${stepsLabel}:`);
      if (missing.length) details.push(`    missing updates: ${missing.join(", ")}`);
      if (extra.length)   details.push(`    extra updates:   ${extra.join(", ")}`);
    }
    return { ok: false, label, details };
  } catch (e) {
    return { ok: false, label, details: [`error: ${e.message}`] };
  }
}

async function check10SolrCore() {
  const label = "Solr core in production up to date";
  try {
    const jsonFacet = JSON.stringify({
      by_registry: { type: "terms", field: "registry", limit: -1, facet: { last_created: "max(created_in_dimensions)" } },
    });

    const params = new URLSearchParams({
      q: "*:*", "q.op": "OR", rows: "0",
      fl: "id,created_in_dimensions",
      facet: "true", "facet.field": "registry", "facet.limit": "-1",
      "json.facet": jsonFacet,
    });

    const res = await fetch(`${SOLR_CORE_URL}?${params}`, {
      headers: { Authorization: SOLR_CORE_AUTH },
    });

    if (!res.ok) {
      return { ok: false, label, details: [`HTTP ${res.status}`] };
    }

    const json    = await res.json();
    const buckets = json?.facets?.by_registry?.buckets ?? [];
    if (!buckets.length) {
      return { ok: false, label, details: ["no registry data in response"] };
    }

    const staleRegistries = [];
    let globalMax = 0;

    for (const b of buckets) {
      const ts = new Date(b.last_created).getTime();
      if (ts > globalMax) globalMax = ts;
      const ageDays = ageDaysFromNow(b.last_created);
      if (ageDays > 6) {
        staleRegistries.push({ val: b.val, ageDays: Math.floor(ageDays), date: b.last_created.slice(0, 10) });
      }
    }

    const globalAgeDays = ageDaysFromNow(globalMax);
    const globalOk  = globalAgeDays <= 7;
    const perRegOk  = staleRegistries.length === 0;
    const ok        = globalOk && perRegOk;

    if (ok) return { ok: true, label, details: [] };

    const details = [];
    if (!globalOk) {
      details.push(`Overall: most recent created_in_dimensions is ${new Date(globalMax).toISOString().slice(0, 10)} (${weekLabel(globalAgeDays)} ago)`);
    }
    if (!perRegOk) {
      details.push(`Stale registries:`);
      for (const r of staleRegistries) details.push(`  - ${r.val}: ${r.date} (${weekLabel(r.ageDays)} ago)`);
    }
    return { ok: false, label, details };
  } catch (e) {
    return { ok: false, label, details: [`error: ${e.message}`] };
  }
}

// ── main checklist ────────────────────────────────────────────────────────────

export async function runChecklist(version, date_release_build) {
  const checks = await Promise.all([
    check1CrawlersFreshness(version),
    check2CrawlersMapped(version),
    check3BulkExport(date_release_build),
    check4SolrEnrichments(date_release_build),
    check5SolrSyncProd(version),
    check6PipelineInput(date_release_build),
    check7GbqExport(date_release_build),
    check8IctrpZips(),
    check9PipelineSanity(date_release_build),
    check10SolrCore(),
  ]);

  const allOk = checks.every(c => c.ok);

  // Summary table — one line per check, always visible
  const summaryLines = checks.map((c, i) =>
    `${String(i + 1).padStart(2)}. ${c.ok ? "✅" : "❌"}  ${c.label}`
  );

  // Detail blocks — only for failing checks
  const detailLines = [];
  for (let i = 0; i < checks.length; i++) {
    const c = checks[i];
    if (c.ok || !c.details.length) continue;
    detailLines.push(``, `${String(i + 1).padStart(2)}. ❌  ${c.label}:`);
    for (const d of c.details) detailLines.push(`    ${d}`);
  }

  const lines = [
    `╔══════════════════════════════════════════════════════════════╗`,
    `  🔄  ClinicalTrials Incremental Updates Checklist`,
    `       version: ${version}   build: ${date_release_build}`,
    `╚══════════════════════════════════════════════════════════════╝`,
    ``,
    ...summaryLines,
    ...(detailLines.length ? [``, `── Details ${"─".repeat(49)}`, ...detailLines] : []),
    ``,
    `───────────────────────────────────────────────────────────────`,
    allOk
      ? `✅  All checks passed — incremental updates for ${version} look good!`
      : `❌  Some checks failed — review the details above.`,
  ];

  return { lines, allOk };
}
