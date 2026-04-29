import os
import json
import hashlib
import logging
from datetime import datetime, timezone
from typing import List, Dict, Set

from dedup import SmartDeduplicator

logger = logging.getLogger("Storage")


class IncidentStorage:

    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.incidents_file = os.path.join(data_dir, "incidents.json")
        self.processed_file = os.path.join(data_dir, "processed_urls.json")

        os.makedirs(data_dir, exist_ok=True)

        self.incidents_data = self._load_incidents()
        self.processed_urls = self._load_processed()
        self.dedup = SmartDeduplicator()

    # ─────────────────────────────────
    # SAVE WITH SMART DEDUP
    # ─────────────────────────────────

    def save_incidents(self, new_incidents: List[Dict]) -> int:
        existing = self.incidents_data.get("incidents", [])

        truly_new, merged = self.dedup.deduplicate_incidents(new_incidents, existing)

        # Update merged existing incidents
        if merged:
            existing_map = {inc.get("id", ""): i for i, inc in enumerate(existing)}
            for updated in merged:
                iid = updated.get("id", "")
                if iid in existing_map:
                    existing[existing_map[iid]] = updated

        # Add truly new
        added = 0
        for inc in truly_new:
            h = self._hash(inc)
            inc["id"] = h[:12]
            inc["created_at"] = datetime.now(timezone.utc).isoformat()
            inc["last_updated"] = datetime.now(timezone.utc).isoformat()
            inc["source_count"] = len(inc.get("source_urls", []))
            existing.append(inc)
            added += 1

        if added > 0 or merged:
            self.incidents_data["incidents"] = existing
            self.incidents_data["metadata"]["total_incidents"] = len(existing)
            self.incidents_data["metadata"]["last_updated"] = datetime.now(timezone.utc).isoformat()
            self._save_incidents()

        return added

    # ─────────────────────────────────
    # URL DEDUP
    # ─────────────────────────────────

    def filter_processed(self, articles: List[Dict]) -> List[Dict]:
        result = []
        for art in articles:
            url = art.get("url", "")
            if not url:
                result.append(art)
                continue
            h = hashlib.md5(url.encode()).hexdigest()
            art["url_hash"] = h
            if h not in self.processed_urls:
                result.append(art)
        return result

    def mark_processed(self, urls: List[str]):
        for url in urls:
            if url:
                self.processed_urls.add(hashlib.md5(url.encode()).hexdigest())
        self._save_processed()

    # ─────────────────────────────────
    # STATS
    # ─────────────────────────────────

    def get_stats(self) -> Dict:
        incs = self.incidents_data.get("incidents", [])
        return {
            "total": len(incs),
            "airport": sum(1 for i in incs if i.get("incident_type") == "AIRPORT_ATTACK"),
            "hotel": sum(1 for i in incs if i.get("incident_type") == "HOTEL_ATTACK"),
            "airline_personnel": sum(1 for i in incs if i.get("incident_type") == "AIRLINE_PERSONNEL"),
            "countries": len(set(i.get("country", "unknown") for i in incs)),
            "verified": sum(1 for i in incs if i.get("verification_status") == "verified"),
        }

    # ─────────────────────────────────
    # INTERNAL
    # ─────────────────────────────────

    def _hash(self, inc: Dict) -> str:
        # date alanı güvenilmez olduğundan hash'ten çıkarıldı.
        # Aynı olay farklı tarihlerle geldiğinde duplikasyon önlenir.
        venue = inc.get("airport_name") or inc.get("hotel_name") or inc.get("venue_name") or ""
        parts = [
            inc.get("country", ""),
            inc.get("city", ""),
            inc.get("incident_type", ""),
            venue,
        ]
        return hashlib.sha256("|".join(str(p).lower().strip() for p in parts).encode()).hexdigest()

    def _load_incidents(self) -> Dict:
        if os.path.exists(self.incidents_file):
            try:
                with open(self.incidents_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if "incidents" in data and "metadata" in data:
                        return data
            except Exception:
                pass

        return {
            "metadata": {
                "project": "Security Incident Monitor",
                "description": "Airport attacks, airline personnel incidents, hotel attacks database",
                "version": "2.0",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "last_updated": datetime.now(timezone.utc).isoformat(),
                "total_incidents": 0,
                "categories": {
                    "AIRPORT_ATTACK": "Physical attacks on airports",
                    "AIRLINE_PERSONNEL": "Attacks/assaults on airline workers",
                    "HOTEL_ATTACK": "Physical attacks on hotels",
                },
                "severity_levels": {
                    "critical": "Mass casualties / large-scale attack",
                    "high": "Deaths or serious injuries",
                    "medium": "Injuries or serious threat",
                    "low": "Minor incident / threat only",
                },
            },
            "incidents": [],
        }

    def _save_incidents(self):
        def _sort_key(x):
            # Önce date, yoksa event_date, yoksa publish_date, yoksa created_at
            for field in ["date", "event_date", "publish_date", "created_at"]:
                val = x.get(field, "")
                if val and str(val).lower() not in ("unknown", "null", "", "none"):
                    return str(val)
            return "0000"
        self.incidents_data["incidents"].sort(key=_sort_key, reverse=True)
        with open(self.incidents_file, "w", encoding="utf-8") as f:
            json.dump(self.incidents_data, f, ensure_ascii=False, indent=2)

    def _load_processed(self) -> Set[str]:
        if os.path.exists(self.processed_file):
            try:
                with open(self.processed_file, "r", encoding="utf-8") as f:
                    return set(json.load(f).get("hashes", []))
            except Exception:
                pass
        return set()

    def _save_processed(self):
        hashes = list(self.processed_urls)[-15000:]
        with open(self.processed_file, "w", encoding="utf-8") as f:
            json.dump({"count": len(hashes), "hashes": hashes}, f, indent=2)