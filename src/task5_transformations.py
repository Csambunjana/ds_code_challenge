# ============================================================================
# TASK 5: Further Data Transformations
# ============================================================================
# 5.1 Subsample sr_hex.csv.gz to requests within 1 arc-minute (~1.85 km) of the
#     centroid of a suburb near Atlantis. Boundary is downloaded from
#     OpenStreetMap (Nominatim); centroid is computed with shapely (NOT hardcoded).
# 5.2 Augment the subsample with 2020 hourly wind speed/direction, joined by the
#     hour each request was created. Uses a RESILIENT fetch (retry/backoff/timeout
#     + local cache), with a documented fallback source for the flaky dependency.
# 5.3 Anonymise the augmented subsample: coarsen location to ~500 m and time to
#     ~6 h, drop directly-identifying fields, and quarantine risky records for a
#     manual human review.
#
# INTERPRETATION: "within 1 minute of the centroid" is read as 1 ARC-MINUTE of
# distance (~1.85 km), the standard meaning for a spatial proximity filter.
# The City's own GIS/air-quality endpoints were unreachable at build time, so
# OpenStreetMap (boundary) and Open-Meteo (wind) are used as documented,
# programmatically-downloaded external sources, as the brief permits.
# ============================================================================


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Piece 1: imports, logging, config, robust paths
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
import os                                                # path resolution
import io                                                # in-memory bytes buffer
import gzip                                              # decompress .csv.gz
import time                                              # timings
import logging                                           # structured logging
import boto3                                             # S3 access
import requests                                          # download external data
import pandas as pd                                      # dataframes
import h3                                                # H3 spatial indexing (for ~500m anonymisation)
from shapely.geometry import shape                       # build geometry + centroid

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # repo root

BUCKET = "cct-ds-code-challenge-input-data"
SR_HEX_KEY = "sr_hex.csv.gz"                              # source data (already has h3 index)
REGION = "af-south-1"
PROFILE = "cct"

SUBURB_QUERY = "Atlantis, City of Cape Town, South Africa"   # suburb near Atlantis
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

ONE_ARC_MINUTE_DEG = 1.0 / 60.0                          # 1 arc-minute in degrees (~1.85 km)


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Piece 2: S3 helper + read the gzipped source data
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
def get_s3_client():                                     # S3 client via the 'cct' profile
    return boto3.Session(profile_name=PROFILE).client("s3", region_name=REGION)


def read_csv_gz_from_s3(s3, key):                        # download + read a .csv.gz
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    raw = obj["Body"].read()
    with gzip.open(io.BytesIO(raw), "rt") as f:
        return pd.read_csv(f)


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Piece 3: download the suburb boundary + compute its centroid (programmatic)
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
def get_suburb_centroid(query):                          # returns (lat, lon) of the centroid
    params = {"q": query, "format": "geojson", "polygon_geojson": 1, "limit": 1}
    headers = {"User-Agent": "cct-ds-challenge/1.0"}     # Nominatim requires a UA
    resp = requests.get(NOMINATIM_URL, params=params, headers=headers, timeout=30)
    resp.raise_for_status()
    feature = resp.json()["features"][0]                 # best match
    geom = shape(feature["geometry"])                    # shapely geometry from GeoJSON
    centroid = geom.centroid                             # COMPUTE the centroid
    logger.info(f"Suburb '{query}' centroid: lat={centroid.y:.6f}, lon={centroid.x:.6f}")
    return centroid.y, centroid.x                        # shapely: x=lon, y=lat


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Piece 4: filter requests within 1 arc-minute of the centroid
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
def subsample_near_centroid(df, c_lat, c_lon, radius_deg):
    df = df.copy()
    df = df[df["latitude"].notna() & df["longitude"].notna()]  # need coords to measure distance
    d_lat = df["latitude"] - c_lat
    d_lon = df["longitude"] - c_lon
    dist_deg = (d_lat ** 2 + d_lon ** 2) ** 0.5          # Euclidean distance in degrees (fine at km scale)
    return df[dist_deg <= radius_deg]                    # keep those within the radius


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Piece 5: RESILIENT fetch helper - retries, exponential backoff, timeout
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# WHY this strategy: the air-quality endpoint is unreliable. Per-attempt
# timeouts stop us hanging on a slow host; exponential backoff recovers
# transient failures (timeouts/5xx); combined with the on-disk cache in
# get_wind_data_2020, re-runs don't depend on the endpoint being up again.
def fetch_with_resilience(url, params=None, headers=None, retries=3, timeout=30):
    for attempt in range(1, retries + 1):               # try up to `retries` times
        try:
            r = requests.get(url, params=params, headers=headers, timeout=timeout)
            r.raise_for_status()                         # raise on 4xx/5xx
            return r                                     # success
        except Exception as e:                           # any network/HTTP error
            wait = 2 ** attempt                          # backoff: 2s, 4s, 8s
            logger.warning(f"Fetch attempt {attempt}/{retries} failed ({e}); retrying in {wait}s")
            time.sleep(wait)
    raise RuntimeError(f"All {retries} attempts failed for {url}")


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Piece 6: get Atlantis 2020 hourly wind data (cached), with documented fallback
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
def get_wind_data_2020(c_lat, c_lon):
    cache_path = os.path.join(ROOT, "data", "atlantis_wind_2020.csv")  # local cache
    if os.path.exists(cache_path):                       # reuse if already downloaded
        logger.info(f"Using cached wind data: {cache_path}")
        return pd.read_csv(cache_path, parse_dates=["time"])

    # Primary source (CCT air-quality portal) is unreliable/moved; we fall back
    # to Open-Meteo's historical archive for Atlantis's coordinates, which
    # reliably serves 2020 hourly wind speed + direction (no API key needed).
    logger.info("Downloading 2020 Atlantis wind data (Open-Meteo fallback)")
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": round(c_lat, 4),
        "longitude": round(c_lon, 4),
        "start_date": "2020-01-01",
        "end_date": "2020-12-31",
        "hourly": "wind_speed_10m,wind_direction_10m",
        "timezone": "Africa/Johannesburg",
    }
    resp = fetch_with_resilience(url, params=params)     # resilient GET
    h = resp.json()["hourly"]
    wind = pd.DataFrame({
        "time": pd.to_datetime(h["time"]),
        "wind_speed": h["wind_speed_10m"],
        "wind_direction": h["wind_direction_10m"],
    })
    wind.to_csv(cache_path, index=False)                 # cache for future runs
    logger.info(f"Fetched {len(wind)} hourly wind records; cached -> {cache_path}")
    return wind


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Piece 7: join wind data to the subsample by the notification creation hour
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
def augment_with_wind(subsample, wind):
    df = subsample.copy()
    # parse creation timestamp (tz-aware), convert to local time, floor to hour
    df["creation_timestamp"] = pd.to_datetime(df["creation_timestamp"], utc=True, errors="coerce")
    df["join_hour"] = (df["creation_timestamp"]
                       .dt.tz_convert("Africa/Johannesburg")
                       .dt.tz_localize(None)
                       .dt.floor("h"))
    wind = wind.copy()
    wind["join_hour"] = pd.to_datetime(wind["time"]).dt.floor("h")
    merged = df.merge(wind[["join_hour", "wind_speed", "wind_direction"]],
                      on="join_hour", how="left")        # attach wind by hour
    matched = int(merged["wind_speed"].notna().sum())
    logger.info(f"Wind join: {matched}/{len(merged)} requests matched a wind record")
    return merged


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Piece 8: ANONYMISATION - coarsen location (~500m) & time (~6h), drop PII
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Location: replace exact lat/lon with an H3 resolution-9 index (~174 m edge,
#   ~0.1 km2 cells) which keeps ~500 m accuracy while removing the exact point.
# Time: floor creation timestamp to a 6-hour bucket.
# Direct identifiers: drop free-text / reference fields that could re-identify a
#   resident. Records still deemed risky are quarantined for manual review.
def anonymise(df):
    df = df.copy()

    # --- location: exact lat/lon -> H3 res-9 (~500 m precision) ---
    def _h3_res9(lat, lon):
        if pd.isna(lat) or pd.isna(lon):
            return "0"
        return h3.latlng_to_cell(lat, lon, 9)            # res-9 ~ 500 m precision
    df["h3_level9_index"] = [
        _h3_res9(lat, lon) for lat, lon in zip(df["latitude"], df["longitude"])
    ]

    # --- time: floor creation timestamp to a 6-hour bucket ---
    ts = pd.to_datetime(df["creation_timestamp"], utc=True, errors="coerce")
    df["creation_6h_bucket"] = ts.dt.floor("6h")

    # --- drop direct/quasi identifiers that would defeat the coarsening ---
    drop_cols = [c for c in [
        "latitude", "longitude",          # exact location (replaced by h3 res-9)
        "creation_timestamp",             # exact time (replaced by 6h bucket)
        "completion_timestamp",           # exact time
        "reference_number",               # external reference that could re-identify
        "notification_number",            # unique request id
    ] if c in df.columns]
    df = df.drop(columns=drop_cols)

    # --- quarantine potentially-identifying singletons (k=1) ---
    group_size = df.groupby(["creation_6h_bucket", "h3_level9_index"])["h3_level9_index"].transform("size")
    is_singleton = group_size == 1                       # rows alone in their (time,cell) group
    review = df[is_singleton].copy()                     # hold for manual review
    anonymised = df[~is_singleton].copy()                # safe to publish

    logger.info(f"Anonymised: {len(anonymised)} publishable, "
                f"{len(review)} quarantined for manual review")
    return anonymised, review


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Piece 9: orchestrate 5.1 -> 5.2 -> 5.3
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
def main():
    s3 = get_s3_client()
    out_dir = os.path.join(ROOT, "data")
    os.makedirs(out_dir, exist_ok=True)

    # --- 5.1: centroid + subsample within 1 arc-minute ---
    t0 = time.time()
    c_lat, c_lon = get_suburb_centroid(SUBURB_QUERY)
    logger.info(f"Centroid computed in {time.time()-t0:.2f}s")

    t1 = time.time()
    sr = read_csv_gz_from_s3(s3, SR_HEX_KEY)
    logger.info(f"Read {len(sr)} requests in {time.time()-t1:.2f}s")

    sub = subsample_near_centroid(sr, c_lat, c_lon, ONE_ARC_MINUTE_DEG)
    logger.info(f"5.1 subsample within 1 arc-minute: {len(sub)} requests")
    sub.to_csv(os.path.join(out_dir, "atlantis_subsample.csv"), index=False)

    # --- 5.2: augment with 2020 wind (resilient fetch + join) ---
    t2 = time.time()
    wind = get_wind_data_2020(c_lat, c_lon)
    augmented = augment_with_wind(sub, wind)
    logger.info(f"5.2 augmentation done in {time.time()-t2:.2f}s")
    augmented.to_csv(os.path.join(out_dir, "atlantis_subsample_wind.csv"), index=False)

    # --- 5.3: anonymise ---
    t3 = time.time()
    anon, review = anonymise(augmented)
    logger.info(f"5.3 anonymisation done in {time.time()-t3:.2f}s")
    anon.to_csv(os.path.join(out_dir, "atlantis_anonymised.csv"), index=False)
    review.to_csv(os.path.join(out_dir, "atlantis_manual_review.csv"), index=False)
    logger.info(f"Saved: atlantis_anonymised.csv ({len(anon)}), "
                f"atlantis_manual_review.csv ({len(review)})")


if __name__ == "__main__":
    main()
