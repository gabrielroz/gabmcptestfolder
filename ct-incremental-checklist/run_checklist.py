#!/usr/bin/env python3
import asyncio
import os
import sys

from checks import run_checklist


async def main():
    version = os.environ.get("CT_VERSION")
    date_release_build = os.environ.get("CT_BUILD_DATE")

    if not version or not date_release_build:
        print("Error: CT_VERSION and CT_BUILD_DATE environment variables must be set.", file=sys.stderr)
        print("  e.g. CT_VERSION=2.1.13 CT_BUILD_DATE=20260303 python run_checklist.py", file=sys.stderr)
        sys.exit(2)

    lines, all_ok = await run_checklist(version, date_release_build)
    print("\n".join(lines))
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    asyncio.run(main())
