import json
import os
import uuid

import boto3
import rasterio
from rasterio.io import MemoryFile
from rasterio.session import AWSSession
from rasterio.windows import WindowError, from_bounds, Window

COG_BUCKET = os.environ["COG_BUCKET"]
OUTPUT_BUCKET = os.environ["OUTPUT_BUCKET"]
URL_EXPIRY = int(os.environ.get("URL_EXPIRY_SECONDS", 900))  # 15 min default

s3_client = boto3.client("s3")

_KEY_CACHE: list[str] | None = None

'''
Helper function to create bucket key cache for county COGS
'''
def _all_keys() -> list[str]:
    global _KEY_CACHE
    if _KEY_CACHE is not None:
        return _KEY_CACHE
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=COG_BUCKET):
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
    _KEY_CACHE = keys
    return keys

'''
Helper function to find object key with matching FIPS code
'''
def _find_key(fips: str) -> str | None:
    for key in _all_keys():
        if fips in key:
            return key
    return None

'''
API Handler to serve COG subset presigned URLs. Expects query parameters:
- fips: 3-digit county FIPS code (e.g. "081")
- bbox: comma-separated bounding box in EPSG:3857 (e.g. "560686,4140115,560786,4140215")
Returns a presigned URL to the extracted subset COG, or an error message.
'''
def handler(event, context):
    params = event.get("queryStringParameters") or {}
    bbox_str = params.get("bbox")
    fips = params.get("fips")

    if not bbox_str or not fips:
        return {"statusCode": 400, "headers": {"Access-Control-Allow-Origin": "*"}, "body": json.dumps({"error": "bbox and county fips are required"})}

    minx, miny, maxx, maxy = map(float, bbox_str.split(","))

    # find object
    key = _find_key(fips)
    if not key:
        return {"statusCode": 404, "headers": {"Access-Control-Allow-Origin": "*"}, "body": json.dumps({"error": f"No COG found for FIPS {fips}"})}

    aws_session = AWSSession(boto3.Session())
    with rasterio.Env(aws_session, GDAL_HTTP_MERGE_CONSECUTIVE_RANGES="YES", VSI_CACHE=True):
        with rasterio.open(f"s3://{COG_BUCKET}/{key}") as src:
            window = from_bounds(minx, miny, maxx, maxy, transform=src.transform)
            try:
                window = window.intersection(Window(0, 0, src.width, src.height))
            except WindowError:
                return {"statusCode": 400, "headers": {"Access-Control-Allow-Origin": "*"}, "body": json.dumps({
                    "error": f"bbox does not overlap COG extent {tuple(src.bounds)}"
                })}
            data = src.read(window=window)
            profile = src.profile.copy()
            profile.update(
                driver="GTiff", height=data.shape[1], width=data.shape[2],
                transform=src.window_transform(window), tiled=False
            )
            profile.pop("blockxsize", None)
            profile.pop("blockysize", None)
            with MemoryFile() as mem:
                with mem.open(**profile) as dst:
                    dst.write(data)
                output_key = f"subsets/{fips}/{uuid.uuid4()}.tif"
                s3_client.put_object(
                    Bucket=OUTPUT_BUCKET,
                    Key=output_key,
                    Body=mem.read(),
                    ContentType="image/tiff",
                )

    url = s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": OUTPUT_BUCKET, "Key": output_key},
        ExpiresIn=URL_EXPIRY,
    )

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"url": url, "expires_in": URL_EXPIRY, "key": output_key}),
    }
