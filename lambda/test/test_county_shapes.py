"""
Unit tests for the county polygon intersection logic in api.py.

Tests _geom_in_county for every supported county using the centroid of each
county's loaded polygon.  No AWS credentials or network access required.

Run from the project root:
    python lambda/test/test_county_shapes.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

os.environ.setdefault("COG_BUCKET", "dummy")
os.environ.setdefault("OUTPUT_BUCKET", "dummy")

import api
from shapely.geometry import box

FIPS_NAMES = {
    "001": "Alameda",
    "013": "Contra Costa",
    "017": "El Dorado",
    "037": "Los Angeles",
    "041": "Marin",
    "057": "Nevada",
    "059": "Orange",
    "061": "Placer",
    "073": "San Diego",
    "081": "San Mateo",
    "085": "Santa Clara",
    "087": "Santa Cruz",
    "097": "Sonoma",
}

# A tiny box well outside California (EPSG:26910 coords near origin)
_OUTSIDE_GEOM = box(0.0, 0.0, 1.0, 1.0)


def test_centroid_geom_inside_each_county():
    """A 1 km box centred on each county's centroid must intersect that county."""
    api._load_county_shapes()
    for fips, name in FIPS_NAMES.items():
        centroid = api._COUNTY_SHAPES[fips].centroid
        half = 500
        geom = box(centroid.x - half, centroid.y - half, centroid.x + half, centroid.y + half)
        result = api._geom_in_county(fips, geom)
        assert result, f"FAIL  {name} ({fips}): centroid geom should intersect county"
        print(f"PASS  {name:15} ({fips})  centroid geom → intersects")


def test_geom_outside_all_counties():
    """A geometry near 0,0 must not intersect any supported county."""
    api._load_county_shapes()
    for fips, name in FIPS_NAMES.items():
        result = api._geom_in_county(fips, _OUTSIDE_GEOM)
        assert not result, f"FAIL  {name} ({fips}): out-of-California geom should not intersect"
    print("PASS  out-of-California geom does not intersect any county")


def test_unknown_fips_falls_through():
    result = api._geom_in_county("999", _OUTSIDE_GEOM)
    assert result is True, "Unknown FIPS should return True (fall-through)"
    print("PASS  unknown FIPS  → fall-through True")


if __name__ == "__main__":
    test_centroid_geom_inside_each_county()
    test_geom_outside_all_counties()
    test_unknown_fips_falls_through()
    print("\nAll county shape tests passed.")
