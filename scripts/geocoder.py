import os
import json
import time
import logging
from typing import Dict, Optional

import requests

logger = logging.getLogger("Geocoder")


class GeocoderService:

    BASE_URL = "https://nominatim.openstreetmap.org/search"

    def __init__(self, data_dir: str):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "SecurityIncidentMonitor/2.0 (academic-research)"
        })
        self.cache_file = os.path.join(data_dir, "geocache.json")
        self.cache = self._load_cache()

    def geocode(self, query: str) -> Optional[Dict]:
        if not query or query.strip().lower() in ("unknown", "null", "n/a", ""):
            return None

        cache_key = query.lower().strip()
        if cache_key in self.cache:
            return self.cache[cache_key]

        try:
            time.sleep(1.1)

            resp = self.session.get(
                self.BASE_URL,
                params={"q": query, "format": "json", "limit": 1, "addressdetails": 1},
                timeout=10,
            )

            if resp.status_code != 200:
                return None

            results = resp.json()

            if not results:
                simplified = self._simplify(query)
                if simplified != query:
                    return self.geocode(simplified)
                return None

            r = results[0]
            coords = {
                "lat": round(float(r["lat"]), 6),
                "lon": round(float(r["lon"]), 6),
                "display_name": r.get("display_name", ""),
            }

            self.cache[cache_key] = coords
            self._save_cache()
            return coords

        except Exception as e:
            logger.warning(f"Geocode error ({query}): {e}")
            return None

    def _simplify(self, query: str) -> str:
        parts = [p.strip() for p in query.split(",")]
        if len(parts) > 2:
            return ", ".join(parts[-2:])
        return query

    def _load_cache(self) -> Dict:
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
        return {}

    def _save_cache(self):
        try:
            with open(self.cache_file, "w", encoding="utf-8") as f:
                json.dump(self.cache, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"Cache save error: {e}")