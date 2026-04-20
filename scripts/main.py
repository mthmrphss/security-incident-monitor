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
    # Ignore generic terms that break geocoding
    ignore_venues = {"airport", "hotel", "venue", "station", "building", "unknown", "null", "none"}
    
    for field in ["airport_name", "hotel_name", "venue_name"]:
        val = incident.get(field)
        if val and str(val).lower().strip() not in ignore_venues:
            parts.append(val)
            break
            
    for field in ["city", "country"]:
        val = incident.get(field)
        if val and str(val).lower() not in ("unknown", "null", "", "none"):
            parts.append(val)
            
    return ", ".join(parts)


def main():
    logger.info("=" * 60)
    logger.info("SECURITY INCIDENT MONITOR v2.0")
    logger.info("=" * 60)

    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        logger.error("GROQ_API_KEY not found in environment")
        sys.exit(1)

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(root, "config", "sources.yaml")
    data_dir = os.path.join(root, "data")
    os.makedirs(data_dir, exist_ok=True)

    collector = NewsCollector(config_path)
    analyzer = GeminiAnalyzer(api_key)
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
        
    # 3.5 — ENRICH CONTENT
    if new_articles:
        logger.info("PHASE 3.5: Enriching article content...")
        new_articles = collector.enrich_articles(new_articles)
        
    # 4 — AI ANALYSIS (batched)
    logger.info("PHASE 4: AI analysis (3-stage pipeline)...")
    all_incidents = []
    batch_size = 10

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
            logger.info("    Waiting 10s for rate limit...")
            time.sleep(10)

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
        # 5.5 — FIX UNKNOWN DATES
    if all_incidents:
        logger.info("PHASE 5.5: Fixing unknown dates...")
        for inc in all_incidents:
            if not inc.get("date") or inc["date"] in ("unknown", "null", ""):
                # Kaynak makalelerin tarihini kullan
                src_indices = inc.get("source_articles", [])
                for idx in src_indices:
                    if 0 <= idx < len(new_articles):
                        pub = new_articles[idx].get("published", "")
                        if pub and pub != "unknown":
                            inc["date"] = pub[:10]
                            logger.info(f"    Date fixed: {inc['date']} for {inc.get('summary_en', '')[:40]}")
                            break

                # Hâlâ unknown ise bugünün tarihini koy
                if not inc.get("date") or inc["date"] in ("unknown", "null", ""):
                    from datetime import datetime, timezone
                    inc["date"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                    logger.info(f"    Date set to today for {inc.get('summary_en', '')[:40]}")

            # year alanını da düzelt
            if inc.get("date") and inc["date"] != "unknown":
                try:
                    inc["year"] = int(inc["date"][:4])
                except (ValueError, TypeError):
                    inc["year"] = datetime.now(timezone.utc).year     
           # 5.6 — FIX MISSING IATA CODES
    if all_incidents:
        logger.info("PHASE 5.6: Fixing missing IATA codes...")
        from iata_lookup import find_iata

        for inc in all_incidents:
            current_iata = inc.get("airport_iata")
            if not current_iata or current_iata in ("null", "unknown", "", None):
                found = find_iata(
                    inc.get("airport_name", ""),
                    inc.get("city", ""),
                    inc.get("country", ""),
                )
                if found:
                    inc["airport_iata"] = found
                    logger.info(
                        f"    IATA: {found} for {inc.get('airport_name', inc.get('city', ''))}"
                    )         

    # 5.7 — STALENESS DETECTION
    if all_incidents:
        logger.info("PHASE 5.7: Staleness detection...")
        from analyzer import normalize_incident_type
        today = datetime.now(timezone.utc).replace(tzinfo=None)

        stale_count = 0
        removed_count = 0
        filtered_incidents = []

        for inc in all_incidents:
            # LLM is_stale kontrolü
            if inc.get("is_stale"):
                logger.info(f"    STALE (LLM flagged): {inc.get('summary_en', '')[:50]}")
                stale_count += 1
                continue  # Stale olanları tamamen atla

            # Tarih tabanlı staleness kontrolü
            event_date_str = inc.get("event_date") or inc.get("date") or ""
            if event_date_str and event_date_str != "unknown":
                try:
                    event_dt = datetime.strptime(str(event_date_str)[:10], "%Y-%m-%d")
                    age_days = (today - event_dt).days
                    if age_days > 14:
                        logger.info(
                            f"    STALE ({age_days} days old): {inc.get('summary_en', '')[:50]}"
                        )
                        stale_count += 1
                        continue  # 14 günden eski olayları atla
                except (ValueError, TypeError):
                    pass

            filtered_incidents.append(inc)

        all_incidents = filtered_incidents
        logger.info(f"    Stale removed: {stale_count}, remaining: {len(all_incidents)}")

    # 5.8 — INCIDENT_TYPE NORMALIZATION (son savunma hattı)
    if all_incidents:
        logger.info("PHASE 5.8: Normalizing incident types...")
        from analyzer import normalize_incident_type

        for inc in all_incidents:
            old_type = inc.get("incident_type", "")
            new_type = normalize_incident_type(old_type)
            if old_type != new_type:
                logger.info(f"    Type fixed: {old_type} -> {new_type}")
                inc["incident_type"] = new_type

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
