"""
Unit tests for the county polygon intersection logic in api.py.

Tests _bbox_in_county for every supported county using the centroid of each
county's loaded polygon.  No AWS credentials or network access required.

Run from the project root:
    python lambda/test/test_county_shapes.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
# Satisfy module-level os.environ reads in api.py without real AWS buckets
os.environ.setdefault("COG_BUCKET", "dummy")
os.environ.setdefault("OUTPUT_BUCKET", "dummy")

import api  # noqa: E402 — must come after env setup

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

# A tiny bbox well outside California — near 0°N, 0°E in the Atlantic Ocean (EPSG:3857)
_OUTSIDE_BBOX = (0.0, 0.0, 1.0, 1.0)


def test_centroid_bbox_inside_each_county():
    """A 1 km² bbox centred on each county's centroid must intersect that county."""
    api._load_county_shapes()
    for fips, name in FIPS_NAMES.items():
        centroid = api._COUNTY_SHAPES[fips].centroid
        half = 500  # 500 m half-width — well within any county boundary
        result = api._bbox_in_county(
            fips,
            centroid.x - half, centroid.y - half,
            centroid.x + half, centroid.y + half,
        )
        assert result, f"FAIL  {name} ({fips}): centroid bbox should intersect county"
        print(f"PASS  {name:15} ({fips})  centroid bbox → intersects")


def test_bbox_outside_all_counties():
    """A bbox near 0,0 (Atlantic Ocean) must not intersect any supported county."""
    api._load_county_shapes()
    for fips, name in FIPS_NAMES.items():
        result = api._bbox_in_county(fips, *_OUTSIDE_BBOX)
        assert not result, f"FAIL  {name} ({fips}): out-of-California bbox should not intersect"
    print("PASS  out-of-California bbox does not intersect any county")


def test_unknown_fips_falls_through():
    """An unrecognised FIPS code must return True (fall-through to rasterio)."""
    result = api._bbox_in_county("999", *_OUTSIDE_BBOX)
    assert result is True, "Unknown FIPS should return True (fall-through)"
    print("PASS  unknown FIPS  → fall-through True")


if __name__ == "__main__":
    test_centroid_bbox_inside_each_county()
    test_bbox_outside_all_counties()
    test_unknown_fips_falls_through()
    print("\nAll county shape tests passed.")
