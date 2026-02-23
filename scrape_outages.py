#!/usr/bin/env python3
"""
Delaware Power Outage Scraper
Fetches outage data from KUBRA Storm Center (Delmarva Power)
and Delaware Electric Cooperative (via PowerOutage.us fallback).
Outputs GeoJSON to data/outages.geojson
"""

import json
import sys
import time
import os
import re
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from html.parser import HTMLParser

# --- KUBRA CONFIG (Pepco Holdings / Exelon) ---
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

    # KUBRA uses [lng, lat] in geom.p
    point = geom.get("p", [None, None])
    if len(point) >= 2:
        lng, lat = point[0], point[1]
    else:
        return None

    # Check if within Delaware bounds
    if not (DE_BOUNDS["min_lat"] <= lat <= DE_BOUNDS["max_lat"] and
            DE_BOUNDS["min_lng"] <= lng <= DE_BOUNDS["max_lng"]):
        return None

    # Parse fields - handle different KUBRA response formats
    n_out = desc.get("n_out", 0)
    if isinstance(n_out, dict):
        n_out = n_out.get("val", 0)
    if not isinstance(n_out, int):
        try:
            n_out = int(n_out)
        except (ValueError, TypeError):
            n_out = 0

    cust_a = desc.get("cust_a", {})
    if isinstance(cust_a, dict) and n_out == 0:
        n_out = cust_a.get("val", 0)

    cause = desc.get("cause", "Unknown")
    if not isinstance(cause, str):
        cause = "Unknown"
    start_time = desc.get("start_time", "")
    etr = desc.get("etr", "")
    inc_id = desc.get("inc_id", "")

    # Build geometry
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
            "customers_affected": n_out,
            "cause": cause,
            "start_time": start_time,
            "etr": etr,
            "area": "",
            "source": "kubra",
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        }
    }


def scrape_kubra():
    """Main KUBRA scraping logic for Delmarva Power."""
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

        data_info = state.get("data", {})
        data_path = data_info.get("interval_generation_data", "")
        cluster_template = data_info.get("cluster_interval_generation_data", "")

        if not data_path:
            print(f"  No data path found for {label}, skipping.")
            continue

        # Fetch summary
        summary = fetch_kubra_summary(data_path)
        if summary:
            total = 0
            areas = []
            if "summaryFileData" in summary:
                areas = summary["summaryFileData"].get("areas", [])
            elif "file_data" in summary:
                areas = summary["file_data"].get("areas", [])
            for area in areas:
                total += area.get("custs_out", 0)
            print(f"  Summary reports {total} total customers out")

        if not cluster_template:
            print(f"  No cluster data template, skipping tile fetch.")
            continue

        # Fetch tiles covering Delaware at multiple zoom levels
        seen_ids = set()
        for zoom in [8, 10, 12]:
            qks = get_quadkeys_for_bbox(DE_BOUNDS, zoom)
            print(f"  Fetching {len(qks)} tiles at zoom {zoom}...")

            for qk in qks:
                tile_data = fetch_kubra_tile(cluster_template, qk)
                if not tile_data:
                    continue

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

        print(f"  Found {len(features)} Delmarva outages in Delaware")

        if features:
            break

    return features


def scrape_dec():
    """
    Scrape Delaware Electric Cooperative outage data.
    DEC uses Siena Tech which has no public API, so we scrape
    PowerOutage.us as a fallback for county-level DEC data.
    """
    features = []
    print("\nFetching DEC (Delaware Electric Cooperative) data...")

    # DEC serves Kent and Sussex counties. County centroids for map placement.
    county_coords = {
        "Kent": {"lat": 39.10, "lng": -75.50},
        "Sussex": {"lat": 38.68, "lng": -75.35},
    }

    # Try PowerOutage.us utility page for DEC (utility ID 127)
    url = "https://poweroutage.us/area/utility/127"
    try:
        req = Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml",
        })
        with urlopen(req, timeout=30) as resp:
            html = resp.read().decode("utf-8")

        print(f"  Got PowerOutage.us DEC page ({len(html)} bytes)")

        # Parse county-level outage data from the page
        for county, coords in county_coords.items():
            patterns = [
                rf'{county}[^<]*?Delaware[^<]*?(\d[\d,]*)\s*$',
                rf'{county}.*?(\d[\d,]*)\s*customers?\s*without\s*power',
                rf'{county}[,\s]+Delaware.*?(\d[\d,]+)',
            ]
            count = 0
            for pattern in patterns:
                match = re.search(pattern, html, re.IGNORECASE | re.DOTALL | re.MULTILINE)
                if match:
                    count = int(match.group(1).replace(",", ""))
                    break

            if count > 0:
                print(f"  DEC {county} County: {count} customers out")
                features.append({
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [coords["lng"], coords["lat"]]
                    },
                    "properties": {
                        "id": f"dec-{county.lower()}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M')}",
                        "provider": "DE Electric Co-op",
                        "customers_affected": count,
                        "cause": "Unknown",
                        "start_time": "",
                        "etr": "",
                        "area": f"{county} County",
                        "source": "poweroutage.us",
                        "scraped_at": datetime.now(timezone.utc).isoformat(),
                    }
                })

    except (URLError, HTTPError) as e:
        print(f"  Could not fetch PowerOutage.us DEC page: {e}")

    # Fallback: try the state page
    if not features:
        print("  Trying state page fallback...")
        try:
            url2 = "https://poweroutage.us/area/state/delaware"
            req2 = Request(url2, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml",
            })
            with urlopen(req2, timeout=30) as resp2:
                html2 = resp2.read().decode("utf-8")

            print(f"  Got state page ({len(html2)} bytes)")

            # Look for DEC total on state page
            patterns2 = [
                r'Delaware\s*Electric.*?(\d[\d,]+)\s*(?:customers?\s*out|power\s*outages)',
                r'Delaware\s*Electric\s*Co[\-\s]*op.*?(\d[\d,]+)',
            ]
            for pattern in patterns2:
                match = re.search(pattern, html2, re.IGNORECASE | re.DOTALL)
                if match:
                    count = int(match.group(1).replace(",", ""))
                    if count > 0:
                        print(f"  DEC total from state page: {count} customers out")
                        features.append({
                            "type": "Feature",
                            "geometry": {
                                "type": "Point",
                                "coordinates": [-75.40, 38.75]
                            },
                            "properties": {
                                "id": f"dec-total-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M')}",
                                "provider": "DE Electric Co-op",
                                "customers_affected": count,
                                "cause": "Unknown",
                                "start_time": "",
                                "etr": "",
                                "area": "DEC Service Area",
                                "source": "poweroutage.us",
                                "scraped_at": datetime.now(timezone.utc).isoformat(),
                            }
                        })
                    break

        except (URLError, HTTPError) as e:
            print(f"  Could not fetch state page: {e}")

    if not features:
        print("  No DEC outage data found")
    else:
        print(f"  Found {len(features)} DEC outage areas")

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
                {"name": "DE Electric Co-op", "via": "PowerOutage.us", "count": len(dec_features)},
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
