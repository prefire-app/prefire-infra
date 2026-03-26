"""
Integration test against the deployed API Gateway endpoint.

# URL is printed by `cdk deploy` as the ApiUrl output.
# Pass it as an env var or let the script fetch it from CloudFormation:

API_URL=https://<id>.execute-api.<region>.amazonaws.com/prod/ python test/aws_test.py

# Or let the script look it up automatically (requires AWS credentials):
python test/aws_test.py
"""
import json
import os
import sys
from pathlib import Path

import boto3
import requests
from pyproj import Transformer
from shapely.geometry import shape, mapping, box

STACK_NAME = "PrefireStack"
OUTPUT_KEY = "ApiUrl"

_to_wgs84 = Transformer.from_crs("EPSG:26910", "EPSG:4326", always_xy=True)


def get_api_url() -> str:
    url = os.environ.get("API_URL")
    if url:
        return url.rstrip("/")

    print(f"API_URL not set — fetching from CloudFormation stack '{STACK_NAME}'...")
    cf = boto3.client("cloudformation")
    resp = cf.describe_stacks(StackName=STACK_NAME)
    outputs = resp["Stacks"][0].get("Outputs", [])
    for o in outputs:
        if o["OutputKey"] == OUTPUT_KEY:
            return o["OutputValue"].rstrip("/")

    print(f"ERROR: Could not find output '{OUTPUT_KEY}' in stack '{STACK_NAME}'")
    print("Run `cdk deploy` to apply the latest stack (adds the CfnOutput), or set API_URL manually.")
    sys.exit(1)


def _county_centroid_polygon(fips_code: str, half_deg: float = 0.0005):
    """Return a small WGS84 square polygon around the given county's centroid."""
    from shapely.ops import transform as shp_transform
    json_path = Path(__file__).parent.parent / "county_polygons.json"
    data = json.loads(json_path.read_text())
    for feat in data:
        if feat["fips"] == fips_code:
            centroid = shape(feat["geometry"]).centroid
            lon, lat = _to_wgs84.transform(centroid.x, centroid.y)
            return mapping(box(lon - half_deg, lat - half_deg, lon + half_deg, lat + half_deg))
    raise ValueError(f"FIPS {fips_code} not found in county_polygons.json")


def _all_county_centroid_polygons(half_deg: float = 0.005):
    """Return {fips: GeoJSON geometry} for each county centroid as a small square."""
    json_path = Path(__file__).parent.parent / "county_polygons.json"
    data = json.loads(json_path.read_text())
    result = {}
    for feat in data:
        centroid = shape(feat["geometry"]).centroid
        lon, lat = _to_wgs84.transform(centroid.x, centroid.y)
        result[feat["fips"]] = mapping(box(lon - half_deg, lat - half_deg, lon + half_deg, lat + half_deg))
    return result


def test_missing_params(base_url: str):
    resp = requests.post(base_url, json={})
    assert resp.status_code == 400, f"Expected 400, got {resp.status_code}"
    assert "error" in resp.json()
    print("PASS  missing params → 400")


def test_invalid_fips(base_url: str):
    geom = _county_centroid_polygon("081")
    resp = requests.post(base_url, json={"fips": "00000", "geometry": geom})
    assert resp.status_code == 404, f"Expected 404, got {resp.status_code}"
    print("PASS  unknown fips  → 404")


def test_geom_outside_county(base_url: str):
    """A polygon in London must be rejected by the county-intersection check."""
    geom = mapping(box(-0.13, 51.50, -0.12, 51.51))
    resp = requests.post(base_url, json={"fips": "081", "geometry": geom})
    assert resp.status_code == 400, f"Expected 400, got {resp.status_code}"
    body = resp.json()
    assert "does not intersect" in body.get("error", ""), (
        f"Expected county-intersection error, got: {body}"
    )
    print("PASS  geometry outside county → 400 (county check)")


def test_geom_in_each_county(base_url: str):
    """A polygon at each county's centroid must not be rejected by the county check."""
    centroid_geoms = _all_county_centroid_polygons()
    for fips, geom in centroid_geoms.items():
        resp = requests.post(base_url, json={"fips": fips, "geometry": geom})
        body = resp.json()
        assert "does not intersect" not in body.get("error", ""), (
            f"FAIL  FIPS {fips}: centroid polygon was incorrectly rejected by county check"
        )
        print(f"PASS  FIPS {fips}  centroid polygon passed county check (status {resp.status_code})")


def test_valid_request(base_url: str):
    # Small WGS84 polygon at San Mateo centroid (~100 m square)
    geom = _county_centroid_polygon("081", half_deg=0.0005)
    resp = requests.post(base_url, json={"fips": "081", "geometry": geom})
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"

    body = resp.json()
    assert "url" in body, "Response missing 'url'"
    assert "key" in body, "Response missing 'key'"
    assert "expires_in" in body, "Response missing 'expires_in'"
    print(f"PASS  valid request → 200  key={body['key']}")

    tif = requests.get(body["url"])
    tif.raise_for_status()
    assert len(tif.content) > 0, "Downloaded file is empty"

    out_path = os.path.join(os.path.dirname(__file__), "aws_output.tif")
    with open(out_path, "wb") as f:
        f.write(tif.content)
    print(f"      Downloaded {len(tif.content):,} bytes → {out_path}")


if __name__ == "__main__":
    base_url = get_api_url()
    print(f"Testing: {base_url}\n")

    test_missing_params(base_url)
    test_invalid_fips(base_url)
    test_geom_outside_county(base_url)
    test_geom_in_each_county(base_url)
    test_valid_request(base_url)

    print("\nAll tests passed.")
