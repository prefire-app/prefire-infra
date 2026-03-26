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
from shapely.geometry import shape

STACK_NAME = "PrefireStack"
OUTPUT_KEY = "ApiUrl"


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

def _county_centroid_bboxes(half: float = 500.0) -> dict[str, str]:
    json_path = Path(__file__).parent.parent / "county_polygons.json"
    data = json.loads(json_path.read_text())
    bboxes = {}
    for feat in data:
        centroid = shape(feat["geometry"]).centroid
        bboxes[feat["fips"]] = (
            f"{centroid.x - half:.0f},{centroid.y - half:.0f}"
            f",{centroid.x + half:.0f},{centroid.y + half:.0f}"
        )
    return bboxes

def test_missing_params(base_url: str):
    resp = requests.get(base_url)
    assert resp.status_code == 400, f"Expected 400, got {resp.status_code}"
    assert "error" in resp.json()
    print("PASS  missing params → 400")


def test_invalid_fips(base_url: str):
    resp = requests.get(base_url, params={"fips": "00000", "bbox": "560686,4140115,560786,4140215"})
    assert resp.status_code == 404, f"Expected 404, got {resp.status_code}"
    print("PASS  unknown fips  → 404")


def test_bbox_outside_county(base_url: str):
    resp = requests.get(base_url, params={"fips": "081", "bbox": "0,0,1,1"})
    assert resp.status_code == 400, f"Expected 400, got {resp.status_code}"
    body = resp.json()
    assert "does not intersect" in body.get("error", ""), (
        f"Expected county-intersection error, got: {body}"
    )
    print("PASS  bbox outside county → 400 (county check)")


def test_bbox_in_each_county(base_url: str):
    centroid_bboxes = _county_centroid_bboxes()
    for fips, bbox in centroid_bboxes.items():
        resp = requests.get(base_url, params={"fips": fips, "bbox": bbox})
        body = resp.json()
        assert "does not intersect" not in body.get("error", ""), (
            f"FAIL  FIPS {fips}: centroid bbox was incorrectly rejected by county check"
        )
        print(f"PASS  FIPS {fips}  centroid bbox passed county check (status {resp.status_code})")


def test_valid_request(base_url: str):
    # 100 m × 100 m box at San Mateo centroid — small enough for 512 MB limit (COG is 0.6 m/px; half=50 → 167×167 px × 4 bands ≈ 100 KB in memory)
    bbox = _county_centroid_bboxes(half=50)["081"]
    resp = requests.get(base_url, params={"fips": "081", "bbox": bbox})
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"

    body = resp.json()
    assert "url" in body, "Response missing 'url'"
    assert "key" in body, "Response missing 'key'"
    assert "expires_in" in body, "Response missing 'expires_in'"
    print(f"PASS  valid request → 200  key={body['key']}")

    # Download the actual GeoTIFF via the presigned URL
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
    test_bbox_outside_county(base_url)
    test_bbox_in_each_county(base_url)
    test_valid_request(base_url)

    print("\nAll tests passed.")
