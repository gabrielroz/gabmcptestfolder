import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  S3Client,
  ListObjectsV2Command,
  HeadObjectCommand,
} from "@aws-sdk/client-s3";
import { z } from "zod";

const s3 = new S3Client({ region: process.env.AWS_REGION || "us-east-1" });

const server = new McpServer({ name: "datasets-incremental-updates", version: "1.0.0" });

// ─── constants ────────────────────────────────────────────────────────────────

const BUCKET_UPDATES  = "com.uberresearch.data.datasets-updates";
const BUCKET_PIPELINE = "com.uberresearch.data.datasets-pipeline";
const STALE_DAYS      = 15;

// ─── helpers ──────────────────────────────────────────────────────────────────

/** List all common prefixes (sub-folders) under a prefix. */
async function listSubFolders(bucket, prefix) {
  const folders = [];
  let continuationToken;
  do {
    const res = await s3.send(new ListObjectsV2Command({
      Bucket: bucket,
      Prefix: prefix,
      Delimiter: "/",
      ContinuationToken: continuationToken,
    }));
    for (const cp of res.CommonPrefixes ?? []) {
      folders.push(cp.Prefix);
    }
    continuationToken = res.IsTruncated ? res.NextContinuationToken : undefined;
  } while (continuationToken);
  return folders;
}

/** Get the HeadObject metadata for a single key, or null on error. */
async function headObject(bucket, key) {
  try {
    return await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
  } catch {
    return null;
  }
}

/**
 * For a list of folder prefixes, find the one whose most-recently-modified
 * object is the newest, and return { folderPrefix, lastModified, ageDays }.
 */
async function newestFolderByLastModified(bucket, folders) {
  let best = null;
  for (const folder of folders) {
    // list up to 1000 objects in each folder; take the newest
    const res = await s3.send(new ListObjectsV2Command({
      Bucket: bucket,
      Prefix: folder,
      MaxKeys: 1000,
    }));
    for (const obj of res.Contents ?? []) {
      if (!obj.LastModified) continue;
      if (!best || obj.LastModified > best.lastModified) {
        best = { folderPrefix: folder, lastModified: obj.LastModified };
      }
    }
  }
  if (!best) return null;
  const ageDays = (Date.now() - new Date(best.lastModified).getTime()) / 86_400_000;
  return { ...best, ageDays: Math.round(ageDays) };
}

/**
 * Within a flat prefix (no sub-folders), find the newest object whose key
 * contains `substring`. Returns { key, lastModified, ageDays } or null.
 */
async function newestFileMatching(bucket, prefix, substring) {
  let best = null;
  let continuationToken;
  do {
    const res = await s3.send(new ListObjectsV2Command({
      Bucket: bucket,
      Prefix: prefix,
      ContinuationToken: continuationToken,
    }));
    for (const obj of res.Contents ?? []) {
      if (!obj.Key.includes(substring)) continue;
      if (!obj.LastModified) continue;
      if (!best || obj.LastModified > best.lastModified) {
        best = { key: obj.Key, lastModified: obj.LastModified };
      }
    }
    continuationToken = res.IsTruncated ? res.NextContinuationToken : undefined;
  } while (continuationToken);

  if (!best) return null;
  const ageDays = (Date.now() - new Date(best.lastModified).getTime()) / 86_400_000;
  return { ...best, ageDays: Math.round(ageDays) };
}

/** Format a checklist row with ✅ / ❌. */
function row(label, ok, detail) {
  return `${ok ? "✅" : "❌"}  ${label}${detail ? `  — ${detail}` : ""}`;
}

/** Format a date as YYYY-MM-DD. */
function fmt(d) {
  return new Date(d).toISOString().slice(0, 10);
}

/** Produce a standard age detail string. */
function ageDetail(result, notFoundMsg = "not found") {
  if (!result) return notFoundMsg;
  const tooOld = result.ageDays > STALE_DAYS;
  return `last modified ${fmt(result.lastModified)} (${result.ageDays}d ago${tooOld ? " — TOO OLD" : ""})`;
}

// ─── tool ─────────────────────────────────────────────────────────────────────

server.tool(
  "datasets_incremental_updates_checklist",
  "Run the S3 incremental-updates checklist for the Datasets pipeline. " +
  "Checks that key folders and files exist and were recently updated.",
  {
    date_version:          z.string().describe("Date version, e.g. 20250509"),
    figshare_date_version: z.string().describe("Figshare date version, e.g. 20250325"),
    datacite_date_version: z.string().describe("Datacite date version, e.g. 20250328"),
    pipeline_run_version:  z.string().describe("Pipeline run version, e.g. 20250511"),
  },
  async ({ date_version, figshare_date_version, datacite_date_version, pipeline_run_version }) => {

    const lines = [
      `╔══════════════════════════════════════════════════════════════╗`,
      `  📦  Datasets Incremental Updates Checklist`,
      `       date: ${date_version}   figshare: ${figshare_date_version}   datacite: ${datacite_date_version}   pipeline: ${pipeline_run_version}`,
      `╚══════════════════════════════════════════════════════════════╝`,
      ``,
    ];

    let allOk = true;

    const flag = (ok) => { if (!ok) allOk = false; return ok; };

    // ── 1. Base datasets updates ──────────────────────────────────────────────
    lines.push(`── 1. Base datasets updates  (pipeline-input/${date_version}/) ────────`);
    try {
      const prefix  = `pipeline-input/${date_version}/`;
      const folders = await listSubFolders(BUCKET_UPDATES, prefix);

      // folders look like pipeline-input/20250509/0000095-0000097/ — sort lexically, last = newest range
      const rangeFolders = folders.filter(f => /\d{7}-\d{7}\/$/.test(f));
      rangeFolders.sort();

      if (rangeFolders.length === 0) {
        lines.push(row(`No range folders found under ${prefix}`, flag(false), ""));
      } else {
        const lastFolder = rangeFolders[rangeFolders.length - 1];
        const result     = await newestFolderByLastModified(BUCKET_UPDATES, [lastFolder]);
        const ok         = flag(!!result && result.ageDays <= STALE_DAYS);
        lines.push(row(
          `Latest range folder: ${lastFolder.replace(prefix, "")}`,
          ok,
          ageDetail(result),
        ));
      }
    } catch (e) {
      lines.push(row(`Check 1 failed`, flag(false), e.message));
    }

    // ── 2. Figshare updates ───────────────────────────────────────────────────
    lines.push(``, `── 2. Figshare updates  (figshare/${figshare_date_version}/updates/) ──`);
    try {
      const prefix  = `figshare/${figshare_date_version}/updates/`;
      const folders = await listSubFolders(BUCKET_UPDATES, prefix);

      // folders look like 20260203_20260205/
      const dateFolders = folders.filter(f => /\d{8}_\d{8}\/$/.test(f));
      dateFolders.sort();

      if (dateFolders.length === 0) {
        lines.push(row(`No date-range folders found under ${prefix}`, flag(false), ""));
      } else {
        const lastFolder = dateFolders[dateFolders.length - 1];
        const result     = await newestFolderByLastModified(BUCKET_UPDATES, [lastFolder]);
        const ok         = flag(!!result && result.ageDays <= STALE_DAYS);
        lines.push(row(
          `Latest figshare folder: ${lastFolder.replace(prefix, "")}`,
          ok,
          ageDetail(result),
        ));
      }
    } catch (e) {
      lines.push(row(`Check 2 failed`, flag(false), e.message));
    }

    // ── 3. Datacite updates ───────────────────────────────────────────────────
    lines.push(``, `── 3. Datacite updates  (datacite/${datacite_date_version}/generic/) ──`);
    try {
      const prefix  = `datacite/${datacite_date_version}/generic/`;
      const folders = await listSubFolders(BUCKET_UPDATES, prefix);

      // folders look like 20260414_20260420/ — skip 'baseline'
      const dateFolders = folders
        .filter(f => /\d{8}_\d{8}\/$/.test(f) && !f.includes("baseline"));
      dateFolders.sort();

      if (dateFolders.length === 0) {
        lines.push(row(`No date-range folders found under ${prefix}`, flag(false), ""));
      } else {
        const lastFolder = dateFolders[dateFolders.length - 1];
        const result     = await newestFolderByLastModified(BUCKET_UPDATES, [lastFolder]);
        const ok         = flag(!!result && result.ageDays <= STALE_DAYS);
        lines.push(row(
          `Latest datacite folder: ${lastFolder.replace(prefix, "")}`,
          ok,
          ageDetail(result),
        ));
      }
    } catch (e) {
      lines.push(row(`Check 3 failed`, flag(false), e.message));
    }

    // ── 4. Preprocessing pipeline ─────────────────────────────────────────────
    lines.push(``, `── 4. Preprocessing pipeline  (preprocessing-pipeline/${date_version}/) ──`);
    const preprocessPrefix = `preprocessing-pipeline/${date_version}/`;

    const filePatterns = [
      { label: "dsl-search-*  files",       substring: "dsl-search-" },
      { label: "generate-update-*  files",  substring: "generate-update-" },
      { label: "merge-datasets-*  files",   substring: "merge-datasets-" },
    ];

    for (const pattern of filePatterns) {
      try {
        const result = await newestFileMatching(BUCKET_UPDATES, preprocessPrefix, pattern.substring);
        const ok     = flag(!!result && result.ageDays <= STALE_DAYS);
        lines.push(row(pattern.label, ok, ageDetail(result)));
      } catch (e) {
        lines.push(row(pattern.label, flag(false), e.message));
      }
    }

    // ── 5. Pipeline run – FilterSolr folder ───────────────────────────────────
    lines.push(``, `── 5. Pipeline run FilterSolr  (${pipeline_run_version}/FilterSolr/) ──`);
    try {
      const prefix  = `${pipeline_run_version}/FilterSolr/`;
      const folders = await listSubFolders(BUCKET_PIPELINE, prefix);

      if (folders.length === 0) {
        // No sub-folders: check objects directly in the prefix
        const result = await newestFolderByLastModified(BUCKET_PIPELINE, [prefix]);
        const ok     = flag(!!result && result.ageDays <= STALE_DAYS);
        lines.push(row(`FilterSolr contents`, ok, ageDetail(result)));
      } else {
        // Sub-folders present: find the most recently modified one
        const result = await newestFolderByLastModified(BUCKET_PIPELINE, folders);
        const ok     = flag(!!result && result.ageDays <= STALE_DAYS);
        const shortName = result?.folderPrefix?.replace(prefix, "") ?? "?";
        lines.push(row(`Latest sub-folder: ${shortName}`, ok, ageDetail(result)));
      }
    } catch (e) {
      lines.push(row(`Check 5 failed`, flag(false), e.message));
    }

    // ── Summary ───────────────────────────────────────────────────────────────
    lines.push(
      ``,
      `───────────────────────────────────────────────────────────────`,
      allOk
        ? `✅  All checks passed — incremental update for ${date_version} looks good!`
        : `❌  Some checks failed — review the items above before proceeding.`,
    );

    return { content: [{ type: "text", text: lines.join("\n") }] };
  }
);

// ─── start ────────────────────────────────────────────────────────────────────

await server.connect(new StdioServerTransport());