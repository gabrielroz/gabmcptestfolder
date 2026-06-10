import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import { runChecklist } from "./checks.mjs";

const server = new McpServer({ name: "ct-incremental-checklist", version: "1.0.0" });

server.tool(
  "ct_incremental_updates_checklist",
  "Run the ClinicalTrials incremental updates checklist (10 checks): " +
  "1) crawler freshness + __COMPLETED__ marker; " +
  "2) crawler→generic mapping lag ≤3d; " +
  "3) bulk export (ai.dimensions.data) artifact+jsonl freshness; " +
  "4) Solr pipeline enrichments bucket freshness; " +
  "5) Solr sync-prod bucket freshness; " +
  "6) pipeline-input bucket freshness; " +
  "7) GBQ export bucket freshness; " +
  "8) ICTRP zip download freshness ≤30d; " +
  "9) pipeline-runs vs pipeline-input update alignment; " +
  "10) Solr core production registry created_in_dimensions freshness.",
  {
    version:            z.string().describe("Release version, e.g. 2.1.13"),
    date_release_build: z.string().describe("Release build date, e.g. 20260303"),
  },
  async ({ version, date_release_build }) => {
    const { lines } = await runChecklist(version, date_release_build);
    return { content: [{ type: "text", text: lines.join("\n") }] };
  }
);

await server.connect(new StdioServerTransport());
