import json
import os
import uuid
from pathlib import Path

import numpy as np
import boto3
import rasterio
from rasterio.io import MemoryFile
from rasterio.session import AWSSession
from rasterio.windows import WindowError, from_bounds, Window
from shapely.geometry import shape
from shapely.ops import transform
from pyproj import Transformer

_to_utm = Transformer.from_crs("EPSG:4326", "EPSG:26910", always_xy=True)
BUFFER_M = 50  # metres to buffer around the user-drawn polygon

COG_BUCKET = os.environ["COG_BUCKET"]
OUTPUT_BUCKET = os.environ["OUTPUT_BUCKET"]
URL_EXPIRY = int(os.environ.get("URL_EXPIRY_SECONDS", 900))  # 15 min default

s3_client = boto3.client("s3")

_KEY_CACHE: list[str] | None = None
_COUNTY_SHAPES: dict[str, object] = {}


def _load_county_shapes():
    global _COUNTY_SHAPES
    if _COUNTY_SHAPES:
        return
    data = json.loads((Path(__file__).parent / "county_polygons.json").read_text())
    _COUNTY_SHAPES = {feat["fips"]: shape(feat["geometry"]) for feat in data}


def _geom_in_county(fips: str, utm_geom) -> bool:
    _load_county_shapes()
    county_geom = _COUNTY_SHAPES.get(fips)
    if county_geom is None:
        return True  # unknown county — let rasterio decide
    return county_geom.intersects(utm_geom)

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

def _find_key(fips: str) -> str | None:
    for key in _all_keys():
        if fips in key:
            return key
    return None

_CORS_HEADERS = {"Access-Control-Allow-Origin": "*"}

def _err(code: int, msg: str):
    return {"statusCode": code, "headers": _CORS_HEADERS, "body": json.dumps({"error": msg})}

'''
API Handler to serve COG subset presigned URLs.

Expects a POST with JSON body:
{
  "fips": "081",
  "geometry": { "type": "Polygon", "coordinates": [[[-122.42, 37.77], ...]] }
}

The geometry is in WGS84 (EPSG:4326). The handler reprojects it to EPSG:26910,
adds a 50 m buffer, computes the bounding box, extracts the COG subset, and
returns a presigned URL to the result.
'''
def handler(event, context):
    body_raw = event.get("body")
    if not body_raw:
        return _err(400, "Request body is required")
    if isinstance(body_raw, str):
        body = json.loads(body_raw)
    else:
        body = body_raw

    fips = body.get("fips")
    geom_json = body.get("geometry")
    if not fips or not geom_json:
        return _err(400, "fips and geometry are required")

    # Build shapely geometry from user-drawn polygon (WGS84)
    user_geom_wgs84 = shape(geom_json)
    if not user_geom_wgs84.is_valid:
        return _err(400, "Invalid geometry")

    # Reproject to EPSG:26910, then buffer 50 m
    user_geom_utm = transform(_to_utm.transform, user_geom_wgs84)
    buffered = user_geom_utm.buffer(BUFFER_M)

    if not _geom_in_county(fips, buffered):
        return _err(400, "geometry does not intersect the requested county")

    key = _find_key(fips)
    if not key:
        return _err(404, f"No COG found for FIPS {fips}")

    minx, miny, maxx, maxy = buffered.bounds

    aws_session = AWSSession(boto3.Session())
    with rasterio.Env(aws_session, GDAL_HTTP_MERGE_CONSECUTIVE_RANGES="YES", VSI_CACHE=True):
        with rasterio.open(f"s3://{COG_BUCKET}/{key}") as src:
            window = from_bounds(minx, miny, maxx, maxy, transform=src.transform)
            try:
                window = window.intersection(Window(0, 0, src.width, src.height))
            except WindowError:
                return _err(400, f"buffered polygon does not overlap COG extent {tuple(src.bounds)}")
            # Read only RGBN bands (1-4), skip alpha/mask if present
            bands = list(range(1, min(src.count, 4) + 1))
            data = src.read(indexes=bands, window=window)
            nodata = src.nodata
            if nodata is not None:
                valid = np.any(data != nodata)
            else:
                valid = np.any(data != 0)
            if not valid:
                return _err(400, "region contains no valid data (all nodata/zero pixels)")
            profile = src.profile.copy()
            profile.update(
                driver="GTiff", count=len(bands),
                height=data.shape[1], width=data.shape[2],
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
