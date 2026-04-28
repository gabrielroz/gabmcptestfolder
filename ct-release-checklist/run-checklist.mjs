#!/usr/bin/env node
/**
 * Standalone CT Release Checklist runner.
 * Reads CT_VERSION and CT_BUILD_DATE from environment variables,
 * runs all S3 checks, prints a report, and exits 0 (all pass) or 1 (any fail).
 */
import {
  S3Client,
  ListObjectsV2Command,
  HeadObjectCommand,
} from "@aws-sdk/client-s3";

const version = process.env.CT_VERSION;
const date_release_build = process.env.CT_BUILD_DATE;

if (!version || !date_release_build) {
  console.error("Error: CT_VERSION and CT_BUILD_DATE environment variables must be set.");
  console.error("  e.g. CT_VERSION=2.1.13 CT_BUILD_DATE=20260303 node run-checklist.mjs");
  process.exit(2);
}

const s3 = new S3Client({ region: process.env.AWS_REGION || "us-east-1" });

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

async function checkFileRecent(bucket, key, withinDays = 31) {
  try {
    const res = await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
    const age = (Date.now() - new Date(res.LastModified).getTime()) / 86_400_000;
    return { ok: age <= withinDays, lastModified: res.LastModified, ageDays: Math.floor(age) };
  } catch (e) {
    return { ok: false, error: e.code === "NotFound" ? "file not found" : e.message };
  }
}

function row(label, { ok, detail }) {
  const icon = ok ? "✅" : "❌";
  return `${icon}  ${label}${detail ? `  — ${detail}` : ""}`;
}

const versionU = version.replaceAll(".", "_");

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

const [folderResults, fileResults] = await Promise.all([
  Promise.all(folderChecks.map(c => checkFolderExists(c.bucket, c.prefix))),
  Promise.all(fileChecks.map(c => checkFileRecent(c.bucket, c.key))),
]);

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

console.log(lines.join("\n"));
process.exit(allOk ? 0 : 1);
