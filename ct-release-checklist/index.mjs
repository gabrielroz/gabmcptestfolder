import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  S3Client,
  ListObjectsV2Command,
  HeadObjectCommand,
} from "@aws-sdk/client-s3";
import { z } from "zod";

const s3 = new S3Client({ region: process.env.AWS_REGION || "us-east-1" });

if (!process.env.S3_BUCKET)
  throw new Error("S3_BUCKET environment variable is not set.");

const server = new McpServer({ name: "s3-release", version: "1.0.0" });

// ─── helpers ──────────────────────────────────────────────────────────────────

/** Check that a prefix exists and has ≥1 object. Returns { ok, count, error } */
async function checkFolderExists(bucket, prefix) {
  try {
    const res = await s3.send(
      new ListObjectsV2Command({ Bucket: bucket, Prefix: prefix, MaxKeys: 1 })
    );
    const count = res.KeyCount ?? 0;
    return { ok: count > 0, count };
  } catch (e) {
    return { ok: false, count: 0, error: e.message };
  }
}

/** Check that a single object exists and was modified within `withinDays` days. */
async function checkFileRecent(bucket, key, withinDays = 31) {
  try {
    const res = await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
    const age = (Date.now() - new Date(res.LastModified).getTime()) / 86_400_000;
    return { ok: age <= withinDays, lastModified: res.LastModified, ageDays: Math.floor(age) };
  } catch (e) {
    return { ok: false, error: e.code === "NotFound" ? "file not found" : e.message };
  }
}

/** Format a single checklist row. */
function row(label, { ok, detail }) {
  const icon = ok ? "✅" : "❌";
  return `${icon}  ${label}${detail ? `  — ${detail}` : ""}`;
}

// ─── tool ─────────────────────────────────────────────────────────────────────

server.tool(
  "release_checklist",
  "Run the S3 release checklist for ClinicalTrials. Checks that all expected folders are non-empty and that key files exist and are recent.",
  {
    version:            z.string().describe("Release version, e.g. 2.1.13"),
    date_release_build: z.string().describe("Release build date, e.g. 20260303"),
  },
  async ({ version, date_release_build }) => {
    // Derive the underscore variant once (2.1.13 → 2_1_13)
    const versionU = version.replaceAll(".", "_");

    // ── Section 1: folders that must exist and be non-empty ───────────────────
    const folderChecks = [
      {
        label: `com.uberresearch.data.clinicaltrials  /${version}/`,
        bucket: "com.uberresearch.data.clinicaltrials",
        prefix: `${version}/`,
      },
      {
        label: `com.uberresearch.data.clinicaltrials  /k8s/generic/${version}/`,
        bucket: "com.uberresearch.data.clinicaltrials",
        prefix: `k8s/generic/${version}/`,
      },
      {
        label: `com.uberresearch.data.clinicaltrials  /k8s/solr_export/${date_release_build}/`,
        bucket: "com.uberresearch.data.clinicaltrials",
        prefix: `k8s/solr_export/${date_release_build}/`,
      },
      {
        label: `ai.dimensions.data  /clinicaltrials/${date_release_build}/`,
        bucket: "ai.dimensions.data",
        prefix: `clinicaltrials/${date_release_build}/`,
      },
      {
        label: `ai-dimensions-data-deliveries-solr  /data/clinical_trials/${versionU}/`,
        bucket: "ai-dimensions-data-deliveries-solr",
        prefix: `data/clinical_trials/${versionU}/`,
      },
      {
        label: `ai-dimensions-data-deliveries-solr  /data/clinical_trials_registries/${versionU}/`,
        bucket: "ai-dimensions-data-deliveries-solr",
        prefix: `data/clinical_trials_registries/${versionU}/`,
      },
      {
        label: `com.uberresearch.data.clinicaltrials  /k8s/bulk_export/${date_release_build}/`,
        bucket: "com.uberresearch.data.clinicaltrials",
        prefix: `k8s/bulk_export/${date_release_build}/`,
      },
      {
        label: `ai-dimensions-data-deliveries-solr  /solr_configset/clinical_trials/registries/v${version}/`,
        bucket: "ai-dimensions-data-deliveries-solr",
        prefix: `solr_configset/clinical_trials/registries/v${version}/`,
      },
    ];

    // ── Section 2: single files that must exist and be < 31 days old ──────────
    const fileChecks = [
      {
        label: `ai.dimensions.data.releases-index  /clinicaltrials/${version}`,
        bucket: "ai.dimensions.data.releases-index",
        key: `clinicaltrials/${version}`,
      },
      {
        label: `ai.dimensions.data.releases-index  /clinicaltrials/latest`,
        bucket: "ai.dimensions.data.releases-index",
        key: `clinicaltrials/latest`,
      },
      {
        label: `com.uberresearch.data.clinicaltrials  /clinicaltrials_releases/${version}/release.json`,
        bucket: "com.uberresearch.data.clinicaltrials",
        key: `clinicaltrials_releases/${version}/release.json`,
      },
      {
        label: `ai-dimensions-data-deliveries-solr  /solr_configset/clinical_trials/clinical_trials_configset-v${version}.zip`,
        bucket: "ai-dimensions-data-deliveries-solr",
        key: `solr_configset/clinical_trials/clinical_trials_configset-v${version}.zip`,
      },
      {
        label: `ai-dimensions-data-deliveries-solr  /solr_configset/clinical_trials/registries/registries_configset-v${version}.zip`,
        bucket: "ai-dimensions-data-deliveries-solr",
        key: `solr_configset/clinical_trials/registries/registries_configset-v${version}.zip`,
      },
    ];

    // ── Run all checks concurrently ───────────────────────────────────────────
    const [folderResults, fileResults] = await Promise.all([
      Promise.all(folderChecks.map(c => checkFolderExists(c.bucket, c.prefix))),
      Promise.all(fileChecks.map(c => checkFileRecent(c.bucket, c.key))),
    ]);

    // ── Build report ──────────────────────────────────────────────────────────
    const lines = [
      `╔══════════════════════════════════════════════════════════════╗`,
      `  🚀  ClinicalTrials Release Checklist`,
      `       version: ${version}   build: ${date_release_build}`,
      `╚══════════════════════════════════════════════════════════════╝`,
      ``,
      `── Folders (must exist and be non-empty) ───────────────────────`,
    ];

    let allOk = true;
    folderChecks.forEach((c, i) => {
      const r = folderResults[i];
      if (!r.ok) allOk = false;
      const detail = r.error
        ? r.error
        : r.ok
        ? `${r.count}+ object(s) found`
        : "empty or missing";
      lines.push(row(c.label, { ok: r.ok, detail }));
    });

    lines.push(``, `── Files (must exist and be updated within 31 days) ────────────`);

    fileChecks.forEach((c, i) => {
      const r = fileResults[i];
      if (!r.ok) allOk = false;
      const detail = r.error
        ? r.error
        : r.ok
        ? `last modified ${new Date(r.lastModified).toISOString().slice(0, 10)} (${r.ageDays}d ago)`
        : `last modified ${new Date(r.lastModified).toISOString().slice(0, 10)} (${r.ageDays}d ago — TOO OLD)`;
      lines.push(row(c.label, { ok: r.ok, detail }));
    });

    lines.push(
      ``,
      `───────────────────────────────────────────────────────────────`,
      allOk
        ? `✅  All checks passed — release ${version} looks good!`
        : `❌  Some checks failed — review the items above before releasing.`
    );

    return { content: [{ type: "text", text: lines.join("\n") }] };
  }
);

// ─── keep the original copy_folder tool ───────────────────────────────────────

import { CopyObjectCommand } from "@aws-sdk/client-s3";

const BUCKET = process.env.S3_BUCKET;

server.tool(
  "copy_folder",
  "Copy all files from one S3 folder to another. Optionally specify a base path (e.g. k8s/generic).",
  {
    source:      z.string().describe("Source version folder, e.g. 2.1.8"),
    destination: z.string().describe("Destination version folder, e.g. 2.1.9"),
    basePath:    z.string().optional().describe("Optional base path, e.g. k8s/generic. If omitted, copies from/to root."),
  },
  async ({ source, destination, basePath }) => {
    if (!BUCKET)
      return { content: [{ type: "text", text: "Error: S3_BUCKET is not set." }] };

    const prefix  = basePath ? `${basePath}/${source}`      : source;
    const destRoot = basePath ? `${basePath}/${destination}` : destination;

    const { Contents = [] } = await s3.send(
      new ListObjectsV2Command({ Bucket: BUCKET, Prefix: prefix })
    );

    if (!Contents.length)
      return { content: [{ type: "text", text: `Nothing found in s3://${BUCKET}/${prefix}` }] };

    for (const obj of Contents) {
      const destKey = obj.Key.replace(prefix, destRoot);
      await s3.send(new CopyObjectCommand({
        Bucket: BUCKET,
        CopySource: `${BUCKET}/${obj.Key}`,
        Key: destKey,
      }));
    }

    return {
      content: [{ type: "text", text:
        `✓ Copied ${Contents.length} file(s)\n  from: s3://${BUCKET}/${prefix}\n  to:   s3://${BUCKET}/${destRoot}`
      }]
    };
  }
);

await server.connect(new StdioServerTransport());