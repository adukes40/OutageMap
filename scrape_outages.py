#!/usr/bin/env python3
"""
Delaware Power Outage Scraper
Fetches outage data from KUBRA Storm Center (Delmarva Power)
and Delaware Electric Cooperative, outputs GeoJSON.
"""

import json
import sys
import time
import os
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

# --- KUBRA CONFIG (Pepco Holdings / Exelon) ---
# These are the known Exelon KUBRA endpoints to try.
# The scraper will auto-discover which view covers Delaware.
KUBRA_CANDIDATES = [
    {
        "instance_id": "877fd1e9-4162-473f-b782-d8a53a85326b",
        "view_id": "a6cee9e4-312b-4b77-9913-2ae371eb860d",
        "label": "Pepco Holdings (Delmarva/Pepco/ACE)"
    },
]
KUBRA_BASE = "https://kubra.io"

# Delaware bounding box for filtering
DE_BOUNDS = {"min_lat": 38.45, "max_lat": 39.84, "min_lng": -75.79, "max_lng": -75.04}

# --- DEC (Delaware Electric Cooperative) via Siena Tech ---
DEC_OUTAGE_URL = "https://dec.maps.sienatech.com"

USER_AGENT = "DelawareOutageTracker/1.0 (github; civic-project)"
HEADERS = {"User-Agent": USER_AGENT, "Accept": "application/json"}


def fetch_json(url, retries=2):
    """Fetch JSON from a URL with retries."""
    for attempt in range(retries + 1):
        try:
            req = Request(url, headers=HEADERS)
            with urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except (URLError, HTTPError, json.JSONDecodeError) as e:
            print(f"  Attempt {attempt+1} failed for {url[:80]}...: {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
    return None


def fetch_kubra_state(instance_id, view_id):
    """Get the currentState from KUBRA which tells us where the data lives."""
    url = f"{KUBRA_BASE}/stormcenter/api/v1/stormcenters/{instance_id}/views/{view_id}/currentState?preview=false"
    print(f"Fetching KUBRA state: {url[:80]}...")
    return fetch_json(url)


def fetch_kubra_summary(data_path):
    """Fetch the summary data which contains total outage counts."""
    url = f"{KUBRA_BASE}/{data_path}/public/summary-1/data.json"
    print(f"Fetching KUBRA summary: {url[:80]}...")
    return fetch_json(url)


def fetch_kubra_service_areas(regions_path):
    """Fetch service area geometry to confirm coverage."""
    url = f"{KUBRA_BASE}/{regions_path}/serviceareas.json"
    print(f"Fetching service areas: {url[:80]}...")
    return fetch_json(url)


def quadkey_to_tile(qk):
    """Convert a quadkey string to (x, y, zoom) tile coordinates."""
    x, y, z = 0, 0, len(qk)
    for i, ch in enumerate(qk):
        mask = 1 << (z - 1 - i)
        d = int(ch)
        if d & 1:
            x |= mask
        if d & 2:
            y |= mask
    return x, y, z


def tile_to_quadkey(x, y, z):
    """Convert tile coordinates to a quadkey string."""
    qk = []
    for i in range(z, 0, -1):
        d = 0
        mask = 1 << (i - 1)
        if x & mask:
            d += 1
        if y & mask:
            d += 2
        qk.append(str(d))
    return "".join(qk)


def tile_to_lng_lat(x, y, z):
    """Convert tile x,y,z to lng,lat of the NW corner."""
    import math
    n = 2.0 ** z
    lng = x / n * 360.0 - 180.0
    lat = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))
    return lng, lat


def get_quadkeys_for_bbox(bbox, zoom):
    """Generate all quadkeys covering a bounding box at a given zoom level."""
    import math
    n = 2 ** zoom
    def lng_to_x(lng):
        return int((lng + 180.0) / 360.0 * n)
    def lat_to_y(lat):
        lat_rad = math.radians(lat)
        return int((1.0 - math.log(math.tan(lat_rad) + 1.0 / math.cos(lat_rad)) / math.pi) / 2.0 * n)

    x_min = lng_to_x(bbox["min_lng"])
    x_max = lng_to_x(bbox["max_lng"])
    y_min = lat_to_y(bbox["max_lat"])  # note: y is inverted
    y_max = lat_to_y(bbox["min_lat"])

    qks = []
    for x in range(x_min, x_max + 1):
        for y in range(y_min, y_max + 1):
            qks.append(tile_to_quadkey(x, y, zoom))
    return qks


def fetch_kubra_tile(cluster_data_template, quadkey):
    """Fetch outage data for a specific map tile."""
    url = f"{KUBRA_BASE}/{cluster_data_template.replace('{qkh}', quadkey[:4])}/{quadkey}"
    return fetch_json(url, retries=1)


def parse_kubra_outage(raw, provider="Delmarva Power"):
    """Parse a raw KUBRA outage object into our GeoJSON feature format."""
    geom = raw.get("geom", {})
    desc = raw.get("desc", {})

    # KUBRA uses [lng, lat] in geom.p or geom.a (polygon)
    point = geom.get("p", [None, None])
    if len(point) >= 2:
        lng, lat = point[0], point[1]
    else:
        return None

    # Check if within Delaware bounds
    if not (DE_BOUNDS["min_lat"] <= lat <= DE_BOUNDS["max_lat"] and
            DE_BOUNDS["min_lng"] <= lng <= DE_BOUNDS["max_lng"]):
        return None

    # Parse fields
    n_out = desc.get("n_out", desc.get("cust_a", {}).get("val", 0))
    cause = desc.get("cause", "Unknown")
    start_time = desc.get("start_time", "")
    etr = desc.get("etr", "")
    cluster = desc.get("cluster", False)
    inc_id = desc.get("inc_id", "")

    # Build polygon if available
    geom_out = {"type": "Point", "coordinates": [lng, lat]}
    if "a" in geom and geom["a"]:
        try:
            coords = geom["a"]
            if isinstance(coords[0], list):
                geom_out = {"type": "Polygon", "coordinates": [coords]}
        except (IndexError, TypeError):
            pass

    return {
        "type": "Feature",
        "geometry": geom_out,
        "properties": {
            "id": inc_id or f"{lng}-{lat}-{start_time}",
            "provider": provider,
            "customers_affected": n_out if isinstance(n_out, int) else 0,
            "cause": cause if isinstance(cause, str) else "Unknown",
            "start_time": start_time,
            "etr": etr,
            "source": "kubra",
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        }
    }


def scrape_kubra():
    """Main KUBRA scraping logic with auto-discovery."""
    features = []

    for candidate in KUBRA_CANDIDATES:
        iid = candidate["instance_id"]
        vid = candidate["view_id"]
        label = candidate["label"]
        print(f"\nTrying KUBRA candidate: {label}")

        state = fetch_kubra_state(iid, vid)
        if not state:
            print(f"  Could not fetch state for {label}, skipping.")
            continue

        # Extract data path and regions path
        data_info = state.get("data", {})
        data_path = data_info.get("interval_generation_data", "")
        cluster_template = data_info.get("cluster_interval_generation_data", "")

        datastatic = state.get("datastatic", {})
        regions_key = list(datastatic.keys())[0] if datastatic else None
        regions_path = datastatic.get(regions_key, "") if regions_key else ""

        if not data_path:
            print(f"  No data path found for {label}, skipping.")
            continue

        # Fetch summary to see total outages
        summary = fetch_kubra_summary(data_path)
        if summary:
            total = 0
            if "summaryFileData" in summary:
                for area in summary["summaryFileData"].get("areas", []):
                    total += area.get("custs_out", 0)
            elif "file_data" in summary:
                for area in summary["file_data"].get("areas", []):
                    total += area.get("custs_out", 0)
            print(f"  Summary reports {total} total customers out")

        if not cluster_template:
            print(f"  No cluster data template, skipping tile fetch.")
            continue

        # Fetch tiles covering Delaware at zoom levels 8-11
        seen_ids = set()
        for zoom in [8, 10, 12]:
            qks = get_quadkeys_for_bbox(DE_BOUNDS, zoom)
            print(f"  Fetching {len(qks)} tiles at zoom {zoom}...")

            for qk in qks:
                tile_data = fetch_kubra_tile(cluster_template, qk)
                if not tile_data:
                    continue

                # Handle different response formats
                outages_raw = []
                if isinstance(tile_data, dict):
                    outages_raw = tile_data.get("outages", tile_data.get("data", []))
                    if not outages_raw and "geom" in tile_data:
                        outages_raw = [tile_data]
                elif isinstance(tile_data, list):
                    outages_raw = tile_data

                for raw in outages_raw:
                    feature = parse_kubra_outage(raw)
                    if feature:
                        fid = feature["properties"]["id"]
                        if fid not in seen_ids:
                            seen_ids.add(fid)
                            features.append(feature)

                time.sleep(0.2)  # be polite

        print(f"  Found {len(features)} outages in Delaware from {label}")

        if features:
            break  # found data, no need to try other candidates

    return features


def scrape_dec():
    """
    Scrape Delaware Electric Cooperative outage data.
    DEC uses Siena Tech - we try their known API patterns.
    """
    features = []
    print("\nFetching DEC (Delaware Electric Cooperative) data...")

    # Try common Siena Tech API endpoints
    endpoints = [
        f"{DEC_OUTAGE_URL}/data/outages.json",
        f"{DEC_OUTAGE_URL}/api/outages",
        f"{DEC_OUTAGE_URL}/data/alerts.json",
    ]

    for url in endpoints:
        data = fetch_json(url, retries=1)
        if data:
            print(f"  Got DEC data from {url}")
            outages = data if isinstance(data, list) else data.get("outages", data.get("features", []))
            for o in outages:
                # Adapt to whatever structure DEC returns
                if "geometry" in o:
                    # Already GeoJSON-like
                    props = o.get("properties", {})
                    features.append({
                        "type": "Feature",
                        "geometry": o["geometry"],
                        "properties": {
                            "id": props.get("id", f"dec-{len(features)}"),
                            "provider": "DE Electric Co-op",
                            "customers_affected": props.get("customersAffected", props.get("numOut", 0)),
                            "cause": props.get("cause", "Unknown"),
                            "start_time": props.get("startTime", ""),
                            "etr": props.get("etr", props.get("estimatedRestoration", "")),
                            "source": "sienatech",
                            "scraped_at": datetime.now(timezone.utc).isoformat(),
                        }
                    })
            break

    if not features:
        print("  Could not fetch DEC data (Siena Tech endpoints may have changed)")
        print("  DEC data will be empty - check dec.maps.sienatech.com manually")

    return features


def main():
    print(f"=== Delaware Outage Scraper ===")
    print(f"Run time: {datetime.now(timezone.utc).isoformat()}")
    print()

    all_features = []

    # 1. Scrape KUBRA (Delmarva Power)
    kubra_features = scrape_kubra()
    all_features.extend(kubra_features)

    # 2. Scrape DEC
    dec_features = scrape_dec()
    all_features.extend(dec_features)

    # 3. Build GeoJSON
    geojson = {
        "type": "FeatureCollection",
        "metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "sources": [
                {"name": "Delmarva Power", "via": "KUBRA Storm Center", "count": len(kubra_features)},
                {"name": "DE Electric Co-op", "via": "Siena Tech", "count": len(dec_features)},
            ],
            "total_outages": len(all_features),
            "total_customers_affected": sum(
                f["properties"].get("customers_affected", 0) for f in all_features
            ),
        },
        "features": all_features,
    }

    # 4. Write output
    outdir = os.environ.get("OUTPUT_DIR", "data")
    os.makedirs(outdir, exist_ok=True)
    outpath = os.path.join(outdir, "outages.geojson")

    with open(outpath, "w") as f:
        json.dump(geojson, f, indent=2)

    print(f"\nWrote {len(all_features)} outages to {outpath}")
    print(f"Total customers affected: {geojson['metadata']['total_customers_affected']}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
