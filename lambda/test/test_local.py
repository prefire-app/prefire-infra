import json
import os
import sys

import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
os.environ["COG_BUCKET"] = "prefire-dev-cog"
os.environ["OUTPUT_BUCKET"] = "prefire-dev-output"

from api import handler

# Small polygon in San Mateo county (WGS84)
event = {
    "body": json.dumps({
        "fips": "081",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-122.42, 37.55], [-122.42, 37.551], [-122.419, 37.551], [-122.419, 37.55], [-122.42, 37.55]]]
        }
    })
}

result = handler(event, {})
print("Status:", result["statusCode"])

if result["statusCode"] == 200:
    body = json.loads(result["body"])
    print("S3 key:       ", body["key"])
    print("Expires in:   ", body["expires_in"], "seconds")
    print("Presigned URL:", body["url"])

    response = requests.get(body["url"])
    response.raise_for_status()

    out_path = os.path.join(os.path.dirname(__file__), "output.tif")
    with open(out_path, "wb") as f:
        f.write(response.content)
    print(f"Downloaded {len(response.content):,} bytes → {out_path}")
else:
    print("Error:", result["body"])
