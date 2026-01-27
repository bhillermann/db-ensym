#!/usr/bin/env python3
"""
db-nvrmap - Generate EnSym and NVRMap compatible shapefiles from PostGIS data.

This is a thin wrapper that imports from the db_nvrmap package.
"""

import sys
from db_nvrmap.cli import main

if __name__ == "__main__":
    sys.exit(main())
