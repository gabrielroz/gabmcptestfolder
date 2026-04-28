import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { S3Client, CopyObjectCommand, ListObjectsV2Command } from "@aws-sdk/client-s3";
import { z } from "zod";

const s3 = new S3Client({ region: process.env.AWS_REGION || "us-east-1" });
const BUCKET = process.env.S3_BUCKET;
if (!BUCKET) throw new Error("S3_BUCKET environment variable is not set. Please add it to your MCP config in JetBrains.");

const server = new McpServer({ name: "s3-release", version: "1.0.0" });

server.tool(
  "copy_folder",
  "Copy all files from one S3 folder to another. Optionally specify a base path (e.g. k8s/generic).",
  {
    source:      z.string().describe("Source version folder, e.g. 2.1.8"),
    destination: z.string().describe("Destinatieton version folder, e.g. 2.1.9"),
    basePath:    z.string().optional().describe("Optional base path, e.g. k8s/generic. If omitted, copies from/to root."),
  },
  async ({ source, destination, basePath }) => {
    if (!BUCKET) return { content: [{ type: "text", text: "Error: S3_BUCKET is not set in the MCP server environment variables." }] };
    const prefix = basePath
      ? `${basePath}/${source}`
      : source;

    const destRoot = basePath
      ? `${basePath}/${destination}`
      : destination;

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