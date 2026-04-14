import re
import logging
from datetime import datetime
from typing import List, Dict, Tuple, Set, Optional
from difflib import SequenceMatcher

logger = logging.getLogger("Dedup")


class SmartDeduplicator:

    SIMILARITY_THRESHOLD = 0.70

    COUNTRY_ALIASES = {
        "kosova": "kosovo",
        "türkiye": "turkey",
        "turkiye": "turkey",
        "türkei": "turkey",
        "turquie": "turkey",
        "birlesik arap emirlikleri": "united arab emirates",
        "bae": "united arab emirates",
        "uae": "united arab emirates",
        "abd": "united states",
        "usa": "united states",
        "amerika": "united states",
        "ingiltere": "united kingdom",
        "uk": "united kingdom",
        "fransa": "france",
        "almanya": "germany",
        "rusya": "russia",
        "cin": "china",
        "japonya": "japan",
        "misir": "egypt",
        "irak": "iraq",
        "iran": "iran",
        "suriye": "syria",
        "lübnan": "lebanon",
        "lubnan": "lebanon",
        "filistin": "palestine",
        "israil": "israel",
        "suudi arabistan": "saudi arabia",
        "kuveyt": "kuwait",
        "katar": "qatar",
        "yemen": "yemen",
        "libya": "libya",
        "somali": "somalia",
        "afganistan": "afghanistan",
        "pakistan": "pakistan",
        "hindistan": "india",
    }

    LOCATION_ALIASES = {
        "istanbul": ["ist", "konstantinopol", "constantinople"],
        "new york": ["nyc", "ny", "jfk", "lga", "ewr", "new york city"],
        "london": ["lhr", "lgw", "stn", "heathrow", "gatwick"],
        "paris": ["cdg", "ory", "charles de gaulle", "orly"],
        "mogadishu": ["mogadişu", "muqdisho", "xamar"],
        "kabul": ["kabil"],
        "dubai": ["dxb"],
        "los angeles": ["lax", "la"],
        "chicago": ["ord", "mdw", "ohare"],
        "atlanta": ["atl", "hartsfield"],
        "beijing": ["pek", "pekin"],
        "mumbai": ["bom", "bombay"],
        "cairo": ["cai", "kahire"],
        "moscow": ["svo", "dme", "moskova"],
    }

    # ═══════════════════════════════════════
    # MAIN DEDUP
    # ═══════════════════════════════════════

    def deduplicate_incidents(
        self,
        new_incidents: List[Dict],
        existing_incidents: List[Dict],
    ) -> Tuple[List[Dict], List[Dict]]:
        """
        Returns:
            (truly_new, merged_updates)
        """
        truly_new = []
        merged = []

        for new_inc in new_incidents:
            match_found = False

            # Check against existing DB
            for existing in existing_incidents:
                sim = self._event_similarity(new_inc, existing)
                if sim >= self.SIMILARITY_THRESHOLD:
                    updated = self._merge(existing, new_inc)
                    merged.append(updated)
                    match_found = True
                    logger.info(
                        f"  MERGE ({sim:.0%}): {new_inc.get('summary_en', '')[:50]}"
                    )
                    break

            if not match_found:
                # Check against other new incidents
                merged_with_new = False
                for j, existing_new in enumerate(truly_new):
                    sim = self._event_similarity(new_inc, existing_new)
                    if sim >= self.SIMILARITY_THRESHOLD:
                        truly_new[j] = self._merge(existing_new, new_inc)
                        merged_with_new = True
                        logger.info(
                            f"  MERGE-NEW ({sim:.0%}): {new_inc.get('summary_en', '')[:50]}"
                        )
                        break

                if not merged_with_new:
                    truly_new.append(new_inc)
                    logger.info(
                        f"  NEW: {new_inc.get('summary_en', '')[:50]}"
                    )

        return truly_new, merged

    # ═══════════════════════════════════════
    # SIMILARITY
    # ═══════════════════════════════════════

    def _date_similarity(self, d1: str, d2: str) -> float:
        """İki tarih arasındaki benzerlik skorunu hesaplar (YYYY-MM-DD formatı beklenir)."""
        if not d1 or not d2:
            return 0.0

        if d1 == d2:
            return 1.0

        try:
            date1 = datetime.strptime(str(d1)[:10], "%Y-%m-%d")
            date2 = datetime.strptime(str(d2)[:10], "%Y-%m-%d")
            
            delta_days = abs((date1 - date2).days)
            
            if delta_days == 0:
                return 1.0
            elif delta_days == 1:
                return 0.8
            elif delta_days <= 3:
                return 0.5
            elif delta_days <= 7:
                return 0.2
            else:
                return 0.0
                
        except (ValueError, TypeError):
            return 0.0

    def _event_similarity(self, a: Dict, b: Dict) -> float:
        # ── KISA DEVRE: Aynı lokasyon kontrolü ──
        iata1 = (a.get("airport_iata") or "").upper().strip()
        iata2 = (b.get("airport_iata") or "").upper().strip()
        if (
            iata1 and iata2
            and iata1 == iata2
            and iata1 not in ("", "UNKNOWN", "NULL")
            and a.get("incident_type") == b.get("incident_type")
        ):
            date_sim = self._date_similarity(a.get("date", ""), b.get("date", ""))
            text_sim = self._text_similarity(a, b)

            # Aynı havalimanı + aynı gün AMA metin çok farklıysa
            # → FARKLI OLAY olabilir (aynı gün birden fazla saldırı)
            if date_sim >= 0.9 and text_sim < 0.25:
                # Saldırı tipi de farklıysa kesinlikle ayrı olay
                if a.get("attack_type") != b.get("attack_type"):
                    logger.info(
                        f"  IATA match BUT different attack: "
                        f"{a.get('attack_type')} vs {b.get('attack_type')} — KEEP SEPARATE"
                    )
                    return 0.4  # Eşik altında → ayrı olay

                # Saldırı tipi aynı ama metin çok farklı → muhtemelen farklı olay
                logger.info(
                    f"  IATA match BUT text very different ({text_sim:.0%}) — KEEP SEPARATE"
                )
                return 0.4

            # Metin benzer → aynı olayın farklı kaynakları
            if date_sim >= 0.3 and text_sim >= 0.25:
                logger.info(f"  IATA match + similar text ({text_sim:.0%}) — MERGE")
                return 1.0
                
         # ── KISA DEVRE: Aynı otel kontrolü ──
        hotel1 = (a.get("hotel_name") or "").lower().strip()
        hotel2 = (b.get("hotel_name") or "").lower().strip()
        if (
            hotel1 and hotel2
            and hotel1 not in ("unknown", "null", "")
            and hotel2 not in ("unknown", "null", "")
            and SequenceMatcher(None, hotel1, hotel2).ratio() > 0.6
            and a.get("incident_type") == b.get("incident_type")
        ):
            text_sim = self._text_similarity(a, b)

            if text_sim < 0.25:
                logger.info(
                    f"  Hotel match BUT text very different ({text_sim:.0%}) — KEEP SEPARATE"
                )
                return 0.4
            else:
                logger.info(f"  Hotel match + similar text ({text_sim:.0%}) — MERGE")
                return 1.0

        # ── NORMAL BENZERLİK HESABI ──
        date_sim = self._date_similarity(a.get("date", ""), b.get("date", ""))
        loc_sim = self._location_similarity(a, b)
        type_sim = 1.0 if a.get("incident_type") == b.get("incident_type") else 0.1
        text_sim = self._text_similarity(a, b)

        # Konum ağırlığını artır
        return date_sim * 0.20 + loc_sim * 0.35 + type_sim * 0.10 + text_sim * 0.35

    def _location_similarity(self, a: Dict, b: Dict) -> float:
        score = 0.0

        # 1. ÜLKE KONTROLÜ
        c1 = self._norm(a.get("country", ""))
        c2 = self._norm(b.get("country", ""))
        
        if c1 == "unknown": c1 = ""
        if c2 == "unknown": c2 = ""

        if c1 and c2:
            if c1 == c2:
                score += 0.4
            else:
                return 0.0  # Ülkeler kesin farklıysa direkt 0 bas ve çık!
        elif c1 or c2:
            score += 0.1    # Biri dolu diğeri boş/unknown ise ufak bir puan ver

        # 2. ŞEHİR KONTROLÜ
        city1 = self._norm(a.get("city", ""))
        city2 = self._norm(b.get("city", ""))
        if city1 and city2:
            if city1 == city2 or self._are_aliases(city1, city2):
                score += 0.3
            elif SequenceMatcher(None, city1, city2).ratio() > 0.7:
                score += 0.2

        # 3. MEKAN KONTROLÜ
        v1 = self._venue(a)
        v2 = self._venue(b)
        if v1 and v2:
            if SequenceMatcher(None, v1, v2).ratio() > 0.6:
                score += 0.3

        # 4. HAVALİMANI KONTROLÜ
        iata1 = (a.get("airport_iata") or "").upper()
        iata2 = (b.get("airport_iata") or "").upper()
        if iata1 and iata2 and iata1 == iata2:
            score += 0.3

        # EN SON ÇIKIŞ (Tavan limiti ile teslimat)
        return min(score, 1.0)

    def _text_similarity(self, a: Dict, b: Dict) -> float:
        t1 = (a.get("summary_en", "") or a.get("summary_tr", "")).lower()
        t2 = (b.get("summary_en", "") or b.get("summary_tr", "")).lower()

        if not t1 or not t2:
            return 0.3

        ratio = SequenceMatcher(None, t1, t2).ratio()

        w1 = set(self._key_terms(t1))
        w2 = set(self._key_terms(t2))
        if w1 and w2:
            overlap = len(w1 & w2) / max(len(w1 | w2), 1)
            return ratio * 0.6 + overlap * 0.4

        return ratio

    # ═══════════════════════════════════════
    # MERGE
    # ═══════════════════════════════════════

    def _merge(self, primary: Dict, secondary: Dict) -> Dict:
        m = primary.copy()

        fill_fields = [
            "country", "country_code", "city", "location_detail",
            "airport_name", "airport_iata", "hotel_name",
            "venue_name", "perpetrator", "attack_type",
        ]
        for f in fill_fields:
            cur = m.get(f)
            new = secondary.get(f)
            if (not cur or cur in ("unknown", "null", "", None)) and new and new not in ("unknown", "null", "", None):
                m[f] = new

        for nf in ["casualties_dead", "casualties_injured"]:
            try:
                val1 = int(m.get(nf, 0) or 0)
            except (ValueError, TypeError):
                val1 = 0
            try:
                val2 = int(secondary.get(nf, 0) or 0)
            except (ValueError, TypeError):
                val2 = 0
            m[nf] = max(val1, val2)

        sev_order = {"critical": 4, "high": 3, "medium": 2, "low": 1}
        if sev_order.get(secondary.get("severity"), 0) > sev_order.get(m.get("severity"), 0):
            m["severity"] = secondary["severity"]

        existing_urls = set(m.get("source_urls") or [])
        new_urls = set(secondary.get("source_urls") or [])
        m["source_urls"] = list(existing_urls | new_urls)
        m["source_count"] = len(m["source_urls"])

        if not m.get("geo_lat") and secondary.get("geo_lat"):
            m["geo_lat"] = secondary["geo_lat"]
            m["geo_lon"] = secondary["geo_lon"]

        m["last_updated"] = datetime.utcnow().isoformat()

        return m

    # ═══════════════════════════════════════
    # HELPERS
    # ═══════════════════════════════════════

    def _norm(self, s: str) -> str:
        if not s:
            return ""
        s = s.lower().strip()
        s = re.sub(r"[^a-zçğıöşü0-9\s]", "", s)
        s = re.sub(r"\s+", " ", s).strip()

        # Ülke/şehir isim varyasyonları
        s = self.COUNTRY_ALIASES.get(s, s)
        return s
        
    def _are_aliases(self, a: str, b: str) -> bool:
        for canonical, aliases in self.LOCATION_ALIASES.items():
            all_names = [canonical] + aliases
            if a in all_names and b in all_names:
                return True
        return False

    def _venue(self, inc: Dict) -> str:
        for f in ["airport_name", "hotel_name", "venue_name"]:
            v = inc.get(f)
            if v and v not in ("unknown", "null", "", None):
                return self._norm(v)
        return ""

    def _key_terms(self, text: str) -> List[str]:
        stops = {
            "the", "a", "an", "in", "at", "on", "to", "for", "of", "and",
            "or", "is", "was", "were", "has", "had", "be", "been", "with",
            "by", "from", "that", "this", "it", "its", "are", "as", "but",
            "not", "bir", "ve", "de", "da", "ile", "bu", "için",
        }
        words = re.findall(r"[a-zçğıöşü]{3,}", text.lower())
        return [w for w in words if w not in stops]
