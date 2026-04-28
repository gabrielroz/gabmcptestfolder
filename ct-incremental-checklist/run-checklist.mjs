#!/usr/bin/env node
import { runChecklist } from "./checks.mjs";

const version            = process.env.CT_VERSION;
const date_release_build = process.env.CT_BUILD_DATE;

if (!version || !date_release_build) {
  console.error("Error: CT_VERSION and CT_BUILD_DATE environment variables must be set.");
  console.error("  e.g. CT_VERSION=2.1.13 CT_BUILD_DATE=20260303 node run-checklist.mjs");
  process.exit(2);
}

const { lines, allOk } = await runChecklist(version, date_release_build);
console.log(lines.join("\n"));
process.exit(allOk ? 0 : 1);
