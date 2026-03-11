"""
Integration test against the deployed API Gateway endpoint.

Usage
─────
  # URL is printed by `cdk deploy` as the ApiUrl output.
  # Pass it as an env var or let the script fetch it from CloudFormation:

  API_URL=https://<id>.execute-api.<region>.amazonaws.com/prod/ python test/aws_test.py

  # Or let the script look it up automatically (requires AWS credentials):
  python test/aws_test.py
"""
import json
import os
import sys

import boto3
import requests

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


# ── Tests ──────────────────────────────────────────────────────────────────────

def test_missing_params(base_url: str):
    resp = requests.get(base_url)
    assert resp.status_code == 400, f"Expected 400, got {resp.status_code}"
    assert "error" in resp.json()
    print("PASS  missing params → 400")


def test_invalid_fips(base_url: str):
    resp = requests.get(base_url, params={"fips": "00000", "bbox": "560686,4140115,560786,4140215"})
    assert resp.status_code == 404, f"Expected 404, got {resp.status_code}"
    print("PASS  unknown fips  → 404")


def test_bbox_no_overlap(base_url: str):
    resp = requests.get(base_url, params={"fips": "081", "bbox": "0,0,1,1"})
    assert resp.status_code == 400, f"Expected 400, got {resp.status_code}"
    print("PASS  no overlap    → 400")


def test_valid_request(base_url: str):
    resp = requests.get(base_url, params={
        "fips": "081",
        "bbox": "560686,4140115,560786,4140215"
    })
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


# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    base_url = get_api_url()
    print(f"Testing: {base_url}\n")

    test_missing_params(base_url)
    test_invalid_fips(base_url)
    test_bbox_no_overlap(base_url)
    test_valid_request(base_url)

    print("\nAll tests passed.")
