import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import { runChecklist } from "./checks.mjs";

const server = new McpServer({ name: "ct-incremental-checklist", version: "1.0.0" });

server.tool(
  "ct_incremental_updates_checklist",
  "Run the ClinicalTrials incremental updates checklist. Checks crawler freshness and completion, k8s/generic mapping sync, pipeline run alignment, and Solr delivery freshness.",
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
