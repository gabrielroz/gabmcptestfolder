from mcp.server.fastmcp import FastMCP

from checks import run_checklist

mcp = FastMCP("ct-incremental-checklist")


@mcp.tool(
    description=(
        "Run the ClinicalTrials incremental updates checklist (10 checks): "
        "1) crawler freshness + __COMPLETED__ marker; "
        "2) crawler→generic mapping lag ≤3d; "
        "3) bulk export (ai.dimensions.data) artifact+jsonl freshness; "
        "4) Solr pipeline enrichments bucket freshness; "
        "5) Solr sync-prod bucket freshness; "
        "6) pipeline-input bucket freshness; "
        "7) GBQ export bucket freshness; "
        "8) ICTRP zip download freshness ≤30d; "
        "9) pipeline-runs vs pipeline-input update alignment; "
        "10) Solr core production registry created_in_dimensions freshness."
    )
)
async def ct_incremental_updates_checklist(version: str, date_release_build: str) -> str:
    """
    Args:
        version: Release version, e.g. 2.1.13
        date_release_build: Release build date, e.g. 20260303
    """
    lines, _ = await run_checklist(version, date_release_build)
    return "\n".join(lines)


if __name__ == "__main__":
    mcp.run()
