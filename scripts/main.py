import os
import sys
import time
import logging
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from collectors import NewsCollector
from analyzer import GeminiAnalyzer
from geocoder import GeocoderService
from storage import IncidentStorage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("Main")


def build_geocode_query(incident):
    parts = []
    for field in ["venue_name", "airport_name", "hotel_name"]:
        val = incident.get(field)
        if val and val not in ("unknown", "null", "", None):
            parts.append(val)
            break
    for field in ["city", "country"]:
        val = incident.get(field)
        if val and val not in ("unknown", "null", "", None):
            parts.append(val)
    return ", ".join(parts)


def main():
    logger.info("=" * 60)
    logger.info("SECURITY INCIDENT MONITOR v2.0")
    logger.info("=" * 60)

    gemini_key = os.environ.get("GEMINI_API_KEY")
    if not gemini_key:
        logger.error("GEMINI_API_KEY not found in environment")
        sys.exit(1)

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(root, "config", "sources.yaml")
    data_dir = os.path.join(root, "data")
    os.makedirs(data_dir, exist_ok=True)

    collector = NewsCollector(config_path)
    analyzer = GeminiAnalyzer(gemini_key)
    geocoder = GeocoderService(data_dir)
    storage = IncidentStorage(data_dir)

    # 1 — COLLECT
    logger.info("PHASE 1: Collecting from all sources...")
    raw = collector.collect_all()
    logger.info(f"  Raw articles: {len(raw)}")

    # 2 — KEYWORD FILTER
    logger.info("PHASE 2: Keyword filtering...")
    filtered = collector.keyword_filter(raw)
    logger.info(f"  After keyword filter: {len(filtered)}")

    # 3 — URL DEDUP
    logger.info("PHASE 3: URL dedup...")
    new_articles = storage.filter_processed(filtered)
    logger.info(f"  New articles: {len(new_articles)}")

    if not new_articles:
        logger.info("No new articles found. Done.")
        return

    # 4 — AI ANALYSIS (batched)
    logger.info("PHASE 4: AI analysis (3-stage pipeline)...")
    all_incidents = []
    batch_size = 8

    for i in range(0, len(new_articles), batch_size):
        batch = new_articles[i : i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(new_articles) + batch_size - 1) // batch_size
        logger.info(f"  Batch {batch_num}/{total_batches} ({len(batch)} articles)")

        try:
            incidents = analyzer.analyze_batch(batch)
            all_incidents.extend(incidents)
            logger.info(f"    Found {len(incidents)} incidents")
        except Exception as e:
            logger.error(f"    Batch error: {e}")

        if i + batch_size < len(new_articles):
            logger.info("    Waiting 45s for rate limit...")
            time.sleep(45)

    logger.info(f"  Total incidents detected: {len(all_incidents)}")

    # 5 — GEOCODING
    if all_incidents:
        logger.info("PHASE 5: Geocoding...")
        for inc in all_incidents:
            if not inc.get("geo_lat"):
                query = build_geocode_query(inc)
                if query:
                    coords = geocoder.geocode(query)
                    if coords:
                        inc["geo_lat"] = coords["lat"]
                        inc["geo_lon"] = coords["lon"]
                        logger.info(f"    {query} -> ({coords['lat']}, {coords['lon']})")

    # 6 — SAVE (with smart dedup)
    if all_incidents:
        logger.info("PHASE 6: Saving with smart dedup...")
        added = storage.save_incidents(all_incidents)
        logger.info(f"  New incidents saved: {added}")

    # Mark URLs as processed
    urls = [a.get("url", "") for a in new_articles if a.get("url")]
    storage.mark_processed(urls)

    # 7 — SUMMARY
    stats = storage.get_stats()
    logger.info("=" * 60)
    logger.info("SUMMARY")
    logger.info(f"  Total incidents : {stats['total']}")
    logger.info(f"  Airport attacks : {stats['airport']}")
    logger.info(f"  Hotel attacks   : {stats['hotel']}")
    logger.info(f"  Personnel       : {stats['airline_personnel']}")
    logger.info(f"  Countries       : {stats['countries']}")
    logger.info(f"  Verified        : {stats['verified']}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
