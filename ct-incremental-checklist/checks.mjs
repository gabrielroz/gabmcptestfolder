import {
  S3Client,
  ListObjectsV2Command,
  HeadObjectCommand,
} from "@aws-sdk/client-s3";

const s3 = new S3Client({ region: process.env.AWS_REGION || "us-east-1" });

const CT_BUCKET   = "com.uberresearch.data.clinicaltrials";
const SOLR_BUCKET = "ai-dimensions-data-deliveries-solr";
const STALE_DAYS  = 3;

// ── helpers ───────────────────────────────────────────────────────────────────

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

function folderName(prefix) {
  return prefix.replace(/\/$/, "").split("/").pop();
}

function parseDateFolder(name) {
  const m = name.match(/^(\d{4})(\d{2})(\d{2})$/);
  return m ? new Date(`${m[1]}-${m[2]}-${m[3]}T00:00:00Z`) : null;
}

/**
 * Find the most recent sub-folder under `prefix`.
 * Prefers sorting by folder name when names are YYYYMMDD dates (cheap, one ListObjects call).
 * Falls back to scanning object LastModified for non-date-named folders.
 * Returns { prefix, name, ageDays } or null if no sub-folders found.
 */
async function newestSubFolder(bucket, prefix) {
  const folders = await listSubFolders(bucket, prefix);
  if (!folders.length) return null;

  const dated = folders
    .map(f => ({ prefix: f, name: folderName(f), date: parseDateFolder(folderName(f)) }))
    .filter(f => f.date)
    .sort((a, b) => b.date - a.date);

  if (dated.length) {
    const newest = dated[0];
    return { ...newest, ageDays: Math.floor((Date.now() - newest.date.getTime()) / 86_400_000) };
  }

  // Fallback: find newest by object LastModified
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
  if (!best.ts) return null;
  return { prefix: best.prefix, name: best.name, ageDays: Math.floor((Date.now() - best.ts) / 86_400_000) };
}

async function fileExists(bucket, key) {
  try { await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key })); return true; }
  catch { return false; }
}

function row(label, ok, detail) {
  return `${ok ? "✅" : "❌"}  ${label}${detail ? `  — ${detail}` : ""}`;
}

// ── main checklist ────────────────────────────────────────────────────────────

export async function runChecklist(version, date_release_build) {
  const versionU = version.replaceAll(".", "_");

  // ── Check 1: Crawlers — freshness ≤ 3d and __COMPLETED__ present ─────────────
  // For each crawler folder under {version}/, find the newest date sub-folder,
  // confirm it is recent and contains a __COMPLETED__ marker file.
  const crawlerFolders = await listSubFolders(CT_BUCKET, `${version}/`);

  const crawlerResults = await Promise.all(
    crawlerFolders.map(async (crawlerPrefix) => {
      const name = folderName(crawlerPrefix);
      let newest;
      try { newest = await newestSubFolder(CT_BUCKET, crawlerPrefix); }
      catch (e) { return { name, ok: false, detail: `error: ${e.message}`, newestName: null }; }

      if (!newest) return { name, ok: false, detail: "no date folders found", newestName: null };

      const stale     = newest.ageDays > STALE_DAYS;
      const completed = await fileExists(CT_BUCKET, `${newest.prefix}__COMPLETED__`);
      const ok        = !stale && completed;

      let detail = `latest: ${newest.name} (${newest.ageDays}d ago)`;
      if (stale)      detail += " — TOO OLD";
      if (!completed) detail += " — NO __COMPLETED__";

      return { name, ok, detail, newestName: newest.name };
    })
  );

  // ── Check 2: k8s/generic mapping — newest date folder must match crawler ─────
  // For each crawler folder under k8s/generic/{version}/, the newest date
  // sub-folder should equal the crawler's newest date folder from Check 1.
  const genericFolders = await listSubFolders(CT_BUCKET, `k8s/generic/${version}/`);

  const crawlerNewestMap = Object.fromEntries(
    crawlerResults.filter(r => r.newestName).map(r => [r.name, r.newestName])
  );

  const genericResults = await Promise.all(
    genericFolders.map(async (genericPrefix) => {
      const name = folderName(genericPrefix);
      let newest;
      try { newest = await newestSubFolder(CT_BUCKET, genericPrefix); }
      catch (e) { return { name, ok: false, detail: `error: ${e.message}` }; }

      if (!newest) return { name, ok: false, detail: "no date folders found" };

      const expected = crawlerNewestMap[name];
      if (!expected) return { name, ok: false, detail: `no crawler baseline for ${name} under ${version}/` };

      const matches = newest.name === expected;
      return {
        name, ok: matches,
        detail: matches
          ? `in sync: ${newest.name}`
          : `MISMATCH — crawler: ${expected}, generic: ${newest.name}`,
      };
    })
  );

  // ── Check 3: Pipeline runs — AltmetricLookup and ExportSolrLoad must be in sync
  // Both should show the same latest processed folder name.
  const pipelineBase = `k8s/pipeline-runs/${date_release_build}/`;
  const [altNewest, solrLoadNewest] = await Promise.all([
    newestSubFolder(CT_BUCKET, `${pipelineBase}AltmetricLookup/`).catch(() => null),
    newestSubFolder(CT_BUCKET, `${pipelineBase}ExportSolrLoad/`).catch(() => null),
  ]);

  const pipelineOk = !!altNewest && !!solrLoadNewest && altNewest.name === solrLoadNewest.name;
  const pipelineDetail = !altNewest || !solrLoadNewest
    ? `missing data — AltmetricLookup: ${altNewest?.name ?? "none"}, ExportSolrLoad: ${solrLoadNewest?.name ?? "none"}`
    : pipelineOk
    ? `both at ${altNewest.name}`
    : `MISMATCH — AltmetricLookup: ${altNewest.name}, ExportSolrLoad: ${solrLoadNewest.name}`;

  // ── Check 4: Solr deliveries — latest update folder must be ≤ 3 days old ─────
  const solrNewest = await newestSubFolder(SOLR_BUCKET, `data/clinical_trials/${versionU}/`).catch(() => null);
  const solrOk     = !!solrNewest && solrNewest.ageDays <= STALE_DAYS;
  const solrDetail = !solrNewest
    ? "no folders found"
    : `latest: ${solrNewest.name} (${solrNewest.ageDays}d ago)${solrOk ? "" : " — TOO OLD"}`;

  // ── Report ────────────────────────────────────────────────────────────────────
  const allOk =
    crawlerFolders.length > 0 &&
    crawlerResults.every(r => r.ok) &&
    genericResults.every(r => r.ok) &&
    pipelineOk &&
    solrOk;

  const lines = [
    `╔══════════════════════════════════════════════════════════════╗`,
    `  🔄  ClinicalTrials Incremental Updates Checklist`,
    `       version: ${version}   build: ${date_release_build}`,
    `╚══════════════════════════════════════════════════════════════╝`,
    ``,
    `── 1. Crawlers  [${version}/]  freshness ≤${STALE_DAYS}d + __COMPLETED__ ─────`,
  ];

  if (!crawlerFolders.length) {
    lines.push(`❌  No crawler folders found under ${version}/`);
  } else {
    for (const r of crawlerResults) lines.push(row(r.name, r.ok, r.detail));
  }

  lines.push(``, `── 2. k8s/generic mapping  [k8s/generic/${version}/] ──────────────`);
  if (!genericFolders.length) {
    lines.push(`❌  No folders found under k8s/generic/${version}/`);
  } else {
    for (const r of genericResults) lines.push(row(r.name, r.ok, r.detail));
  }

  lines.push(``, `── 3. Pipeline runs sync  [k8s/pipeline-runs/${date_release_build}/] ─`);
  lines.push(row(`AltmetricLookup ↔ ExportSolrLoad`, pipelineOk, pipelineDetail));

  lines.push(``, `── 4. Solr deliveries  [data/clinical_trials/${versionU}/] ─────────`);
  lines.push(row(`Latest update folder`, solrOk, solrDetail));

  lines.push(
    ``,
    `───────────────────────────────────────────────────────────────`,
    allOk
      ? `✅  All checks passed — incremental updates for ${version} look good!`
      : `❌  Some checks failed — review the items above.`
  );

  return { lines, allOk };
}
