# scripts/analyzer.py

"""
2 aşamalı pipeline:
  Aşama 1 — Sınıflandır (ilgili mi?)
  Aşama 2 — Her ilgili haber için TEK TEK veri çıkar

Merge: LLM YAPMAZ, dedup.py yapar.
Confidence: LLM'den değil, çoklu sinyal ile hesaplanır.
Token: Kontrollü, batch boyutu sınırlı.
Tarih: Haber metninden çıkarılır, makale tarihi fallback.
"""

import json
import time
import re
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict

from groq import Groq

logger = logging.getLogger("Analyzer")

# ── TOKEN LİMİTLERİ ──
MAX_CHARS_PER_ARTICLE = 300     # ~75 token
MAX_ARTICLES_PER_BATCH = 10     # Stage 1 için (main.py batch_size ile tutarlı)
MAX_ARTICLES_STAGE2 = 5         # Stage 2 tek tek işler
GROQ_CONTEXT_LIMIT = 5000       # Prompt için güvenli karakter limiti (daha konservatif)

# ── TARİH DOĞRULAMA ──
MIN_VALID_EVENT_YEAR = datetime.now(timezone.utc).year  # Dinamik: her yıl otomatik güncellenir
MAX_FUTURE_DAYS = 2             # Bugünden en fazla bu kadar gün ilerideki tarihler kabul edilir

# ── BİLİNEN ESKİ OLAYLAR (Google News link maskeleme sorunu) ──
# Bu anahtar kelimeler geçen olaylar yıl kontrolüne tabi tutulur
KNOWN_HISTORICAL_EVENTS = [
    ("mali", "hotel", "21 dead"),
    ("mali", "radisson", "hostage"),
    ("mumbai", "taj", "2008"),
    ("mumbai", "terrorist", "hotel"),
    ("westgate", "nairobi", "mall"),
    ("bataclan", "paris", "attack"),
    ("brussels", "airport", "2016"),
    ("istanbul", "ataturk", "2016"),
]

# ── KATEGORİ NORMALİZASYONU ──
CATEGORY_MAP = {"A": "AIRPORT_ATTACK", "B": "AIRLINE_PERSONNEL", "C": "HOTEL_ATTACK"}
VALID_INCIDENT_TYPES = {"AIRPORT_ATTACK", "AIRLINE_PERSONNEL", "HOTEL_ATTACK"}

def normalize_incident_type(raw_type: str) -> str:
    """A/B/C veya diğer geçersiz değerleri doğru kategori ismine çevir."""
    if not raw_type:
        return "AIRPORT_ATTACK"
    cleaned = raw_type.strip().upper()
    if cleaned in CATEGORY_MAP:
        return CATEGORY_MAP[cleaned]
    if cleaned in VALID_INCIDENT_TYPES:
        return cleaned
    # Kısmi eşleşme dene
    for valid in VALID_INCIDENT_TYPES:
        if cleaned in valid or valid in cleaned:
            return valid
    return "AIRPORT_ATTACK"  # Güvenli varsayılan


class GeminiAnalyzer:

    def __init__(self, api_key: str):
        self.client = Groq(api_key=api_key)

        self._model_names = [
            "llama-3.3-70b-versatile",
            "meta-llama/llama-4-scout-17b-16e-instruct",
            "llama-3.1-8b-instant",  # Hızlı, ucuz fallback — son çare
        ]

        self._last_request_time = 0
        self._min_interval = 4   # Groq free tier güvenliği için 3→4
        self._max_retries = 3

    # ═══════════════════════════════════════
    # RATE LIMITER
    # ═══════════════════════════════════════

    def _wait(self):
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_request_time = time.time()

    def _call_api(self, prompt: str, label: str = "") -> dict:
        # ── Token overflow koruması ──
        if len(prompt) > GROQ_CONTEXT_LIMIT * 2:
            logger.warning(
                f"      Prompt too long ({len(prompt)} chars), truncating..."
            )
            prompt = prompt[:GROQ_CONTEXT_LIMIT * 2]

        for model_name in self._model_names:
            for attempt in range(self._max_retries):
                try:
                    self._wait()
                    logger.info(f"      API: {model_name} (try {attempt+1})")

                    response = self.client.chat.completions.create(
                        model=model_name,
                        messages=[
                            {
                                "role": "system",
                                "content": (
                                    "You are a security incident analyst. "
                                    "Respond ONLY in valid JSON. "
                                    "No markdown, no explanation."
                                )
                            },
                            {"role": "user", "content": prompt}
                        ],
                        temperature=0.1,
                        max_tokens=1024,
                        response_format={"type": "json_object"},
                    )

                    text = response.choices[0].message.content
                    if not text:
                        continue

                    self._last_request_time = time.time()
                    return json.loads(text)

                except Exception as e:
                    err = str(e).lower()
                    if "429" in str(e) or "rate" in err or "limit" in err:
                        retry_match = re.search(r"try again in (\d+\.?\d*)", err)
                        wait = float(retry_match.group(1)) + 2 if retry_match else 15 * (attempt + 1)
                        logger.warning(f"      Rate limited, waiting {wait:.0f}s")
                        time.sleep(wait)
                        if attempt == self._max_retries - 1:
                            logger.warning(f"      {model_name} exhausted, next model...")
                    else:
                        logger.warning(f"      {model_name} error: {e}")
                        if attempt < self._max_retries - 1:
                            time.sleep(5)
            time.sleep(2)

        logger.error(f"      ALL models failed: {label}")
        return {}

    # ═══════════════════════════════════════
    # ANA PIPELINE — 2 AŞAMALI
    # ═══════════════════════════════════════

    def analyze_batch(self, articles: List[Dict]) -> List[Dict]:
        """
        Aşama 1: Toplu sınıflandırma (ilgili mi?)
        Aşama 2: Her ilgili haber için TEK TEK veri çıkarma
        Merge yok — dedup.py halleder
        """

        # ── Token koruması: batch boyutunu sınırla ──
        if len(articles) > MAX_ARTICLES_PER_BATCH:
            logger.warning(
                f"    Batch too large ({len(articles)}), "
                f"trimming to {MAX_ARTICLES_PER_BATCH}"
            )
            articles = articles[:MAX_ARTICLES_PER_BATCH]

        # ════════════════════════════
        # AŞAMA 1 — SINIFLANDIRMA
        # ════════════════════════════
        logger.info(f"    Stage 1: Classifying {len(articles)} articles...")
        relevant = self._stage1_classify(articles)
        logger.info(f"      {len(relevant)}/{len(articles)} relevant")

        if not relevant:
            return []

        # En fazla MAX_ARTICLES_STAGE2 kadar işle
        if len(relevant) > MAX_ARTICLES_STAGE2:
            logger.warning(
                f"    Too many relevant ({len(relevant)}), "
                f"taking top {MAX_ARTICLES_STAGE2}"
            )
            relevant = relevant[:MAX_ARTICLES_STAGE2]

        # ════════════════════════════
        # AŞAMA 2 — TEK TEK ÇIKARMA
        # ════════════════════════════
        logger.info(f"    Stage 2: Extracting {len(relevant)} incidents...")
        all_incidents = []

        for i, article in enumerate(relevant):
            logger.info(
                f"      [{i+1}/{len(relevant)}] "
                f"{article.get('title', '')[:60]}"
            )
            incident = self._stage2_extract_single(article)
            if incident:
                # ── Çoklu sinyal doğrulama ──
                quality = self._validate_incident(incident, article)
                incident["validation_signals"] = quality["signals"]
                incident["quality_score"] = quality["score"]
                incident["verification_status"] = quality["status"]
                incident["verification_score"] = quality["score"]

                if quality["score"] >= 0.5:
                    all_incidents.append(incident)
                    logger.info(
                        f"        ✅ Quality {quality['score']:.0%}: "
                        f"{incident.get('summary_en', '')[:50]}"
                    )
                else:
                    logger.info(
                        f"        ❌ Low quality {quality['score']:.0%}: "
                        f"{incident.get('summary_en', '')[:50]} "
                        f"— {quality['signals']}"
                    )

        logger.info(f"    Result: {len(all_incidents)} validated incidents")
        return all_incidents

    # ═══════════════════════════════════════
    # AŞAMA 1 — SINIFLANDIRMA
    # ═══════════════════════════════════════

    def _stage1_classify(self, articles: List[Dict]) -> List[Dict]:
        """Toplu sınıflandırma — sadece ilgili/ilgisiz."""

        articles_text = ""
        for i, art in enumerate(articles):
            title = art.get("title", "")[:120]
            summary = art.get("summary", "")[:150]
            text_block = f"[{i}] {title} | {summary}\n"

            # Token overflow kontrolü
            if len(articles_text) + len(text_block) > GROQ_CONTEXT_LIMIT:
                logger.warning(f"      Token limit, stopping at {i} articles")
                break

            articles_text += text_block

        prompt = f"""Classify each article: is it about a REAL, RECENT physical security incident?

TODAY'S DATE: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}

Categories:
A) AIRPORT_ATTACK — bomb, gun, knife, explosion, drone attack at airport
B) AIRLINE_PERSONNEL — physical assault on cabin crew, pilot, ground staff
C) HOTEL_ATTACK — bomb, armed raid, hostage, explosion at hotel

NOT relevant (MUST filter out):
- Strikes, delays, cancellations, weather
- Accidents, technical failures
- Security tech/product news
- OLD event anniversaries, memorials, documentaries, retrospectives, "looking back" articles
- Court cases, trials, or sentencing for PAST events
- Reviews, tourism, bookings
- Threats deemed "not credible" with nothing found
- Articles about events that happened YEARS AGO (e.g., 2008 Mumbai attacks, 2015 Mali hotel siege, 2016 Brussels bombing) — even if republished recently
- Movie reviews, book reviews, or media about past attacks
- Drills, exercises, simulations, training scenarios

AGGRESSIVE STALE DETECTION RULES (apply STRICTLY):
1. If the article mentions dates like "2015", "2008", "2016", "2020", "2021", "2022" etc. referring to WHEN the event happened, and that year is NOT the current year → it is OLD NEWS and NOT relevant.
2. If the article contains words like "anniversary", "remembering", "memorial", "commemoration", "years since", "look back", "retrospective", "on this day" → it is OLD NEWS and NOT relevant.
3. If the article mentions "drill", "exercise", "simulation", "training" → it is NOT relevant.
4. If the article is about a court case, trial, sentencing, or legal proceedings for a PAST event → it is NOT relevant.
5. If the article's publish date is recent but it describes an event that clearly happened in a DIFFERENT year → it is NOT relevant (republished old news).
6. ONLY set relevant:true if the article clearly describes a RECENT physical attack, assault, bombing, or credible threat that happened in the CURRENT YEAR or very recently (within the last few days).

ARTICLES:
{articles_text}

Return JSON:
{{
  "results": [
    {{"index": 0, "relevant": true, "category": "AIRPORT_ATTACK"}},
    {{"index": 1, "relevant": false}}
  ]
}}

Only set relevant:true if the article clearly describes a RECENT physical attack, assault, bombing, or credible threat that happened in the current year or very recently."""

        result = self._call_api(prompt, "Stage1")

        if not result:
            logger.warning("      Stage 1 failed, passing all articles")
            return articles

        relevant = []
        for r in result.get("results", []):
            idx = r.get("index", -1)
            if r.get("relevant") is True and 0 <= idx < len(articles):
                art = articles[idx].copy()
                # Kategoriyi normalize et (A→AIRPORT_ATTACK, B→AIRLINE_PERSONNEL, C→HOTEL_ATTACK)
                raw_cat = r.get("category", "UNKNOWN")
                art["ai_category"] = normalize_incident_type(raw_cat)
                relevant.append(art)

        return relevant

    # ═══════════════════════════════════════
    # AŞAMA 2 — TEK HABER → TEK OLAY
    # ═══════════════════════════════════════

    def _stage2_extract_single(self, article: Dict) -> dict:
        """
        Tek bir haberden yapılandırılmış olay verisi çıkar.
        LLM'e merge yaptırmıyoruz — her haber = ayrı kayıt.
        """

        title = article.get("title", "")[:200]
        summary = article.get("summary", "")[:500]
        pub_date = article.get("published", "")[:25]
        real_pub_date = article.get("real_publish_date", "")
        source = article.get("source", "")
        url = article.get("url", "")
        category = article.get("ai_category", "UNKNOWN")

        # Prompt'ta en güvenilir tarihi göster
        date_context = ""
        if real_pub_date:
            date_context = f"\nARTICLE REAL PUBLISH DATE (from HTML meta): {real_pub_date}"
        date_context += f"\nRSS/FEED DATE: {pub_date}"

        # Bugünün tarihini prompt'a ekle (LLM'in zaman algısı için)
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        prompt = f"""Extract incident details from this single article.

TODAY'S DATE: {today_str}
SOURCE: {source}{date_context}
CATEGORY: {category}
TITLE: {title}
CONTENT: {summary}
URL: {url}

Return JSON:
{{
  "incident_type": "{category}",
  "event_date": "YYYY-MM-DD from the article text, NOT the publish date if different",
  "publish_date": "{real_pub_date or pub_date[:10]}",
  "country": "Country in English",
  "country_code": "XX",
  "city": "City name",
  "location_detail": "specific location from article",
  "airport_name": "exact airport name from article or null",
  "airport_iata": "IATA code if mentioned or null",
  "hotel_name": "exact hotel name from article or null",
  "venue_name": "general venue name",
  "attack_type": "bombing|shooting|stabbing|assault|explosion|siege|threat|drone|arson|other",
  "severity": "critical|high|medium|low",
  "casualties_dead": 0,
  "casualties_injured": 0,
  "perpetrator": "from article or unknown",
  "summary_en": "what happened in max 150 chars",
  "summary_tr": "Doğal, akıcı Türkçe kullan. Kelime kelime çeviri yapma. Olayın özünü kısa ve net anlat. Teknik terimleri doğru kullan: airport=havalimanı, crew=kabin ekibi, flight attendant=hostes, pilot=pilot, bombing=bombalı saldırı, shooting=silahlı saldırı, stabbing=bıçaklı saldırı, assault=saldırı, hostage=rehine, siege=kuşatma, explosion=patlama, attack=saldırı. Maksimum 150 karakter.",
  "is_ongoing": false,
  "is_false_alarm": false,
  "is_stale": false,
  "original_event_year": null,
  "tags": ["relevant tags"]
}}

STRICT RULES:
- ONLY write what is EXPLICITLY in the article text above.
- If info is NOT in the text, write "unknown" or null. Do NOT guess.
- event_date: extract the ACTUAL event date from text. If text says "yesterday" and publish is {today_str}, event_date should be calculated accordingly.
- publish_date: Use the ARTICLE REAL PUBLISH DATE if provided, otherwise use RSS/FEED DATE.
- If no event date in text, use publish_date as fallback.
- is_false_alarm: true if article says "not credible", "hoax", "nothing found"
- is_stale: true if the article is reporting on an OLD event. DETECTION CRITERIA:
  * The article mentions a year like "2015", "2008", "2016", "2020", "2021", "2022" etc. referring to WHEN the event happened, and that year is NOT the current year.
  * The article contains words like "anniversary", "remembering", "memorial", "commemoration", "years since", "look back", "retrospective", "on this day".
  * The article is about a court case, trial, or sentencing for a past event.
  * The article is a documentary, book review, or movie about a past attack.
  * The article is clearly a republished old news story.
- original_event_year: If the event described happened in a DIFFERENT year than the publish date, write that year (e.g., 2015, 2008). Otherwise null.
  * Court cases or trials about events that happened years ago
- original_event_year: If the event described happened in a DIFFERENT year than the publish date, write that year (e.g., 2015, 2008). Otherwise null.
- Do NOT invent casualty numbers. If not mentioned, use 0.
- incident_type MUST be one of: AIRPORT_ATTACK, AIRLINE_PERSONNEL, HOTEL_ATTACK. Do NOT use letters like A, B, C."""

        result = self._call_api(prompt, f"Stage2-{article.get('title', '')[:30]}")

        if not result:
            return None

        # ── Tarih düzeltme ──
        event_date = result.get("event_date", "")
        publish_date = result.get("publish_date", "")
        original_event_year = result.get("original_event_year")

        # ── ESKİ OLAY TESPİTİ (LLM sinyali) ──
        if original_event_year:
            try:
                oey = int(original_event_year)
                if oey < MIN_VALID_EVENT_YEAR:
                    logger.warning(
                        f"      HISTORICAL EVENT detected (year={oey}): "
                        f"{result.get('summary_en', '')[:60]}"
                    )
                    result["is_stale"] = True
                    return result  # Erken dönüş — stale olarak işaretle
            except (ValueError, TypeError):
                pass

        # Tarihleri normalize et (ISO 8601 basic → YYYY-MM-DD)
        event_date = self._normalize_date(event_date)
        publish_date = self._normalize_date(publish_date)

        # Gerçek yayın tarihi (HTML meta'dan)
        real_date = self._normalize_date(real_pub_date) if real_pub_date else "unknown"

        # RSS/feed tarihi
        article_pub = self._normalize_date(pub_date)

        # ── ESKİ TARİH KONTROLÜ — event_date yıl < MIN_VALID_EVENT_YEAR ──
        if event_date and event_date != "unknown":
            try:
                ed_year = int(event_date[:4])
                if ed_year < MIN_VALID_EVENT_YEAR:
                    logger.warning(
                        f"      OLD EVENT DATE detected ({event_date}): "
                        f"{result.get('summary_en', '')[:60]}"
                    )
                    result["is_stale"] = True
                    return result
            except (ValueError, TypeError):
                pass

        # ── Geliştirilmiş Fallback Zinciri ──
        # Öncelik: event_date > real_publish_date > publish_date (LLM) > RSS date > bugün
        best_date = "unknown"
        for candidate in [event_date, real_date, publish_date, article_pub]:
            if candidate and candidate != "unknown":
                best_date = candidate
                break

        if best_date == "unknown":
            best_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # ── Tarih Tutarlılık Kontrolü ──
        try:
            best_dt = datetime.strptime(best_date, "%Y-%m-%d")
            today = datetime.now(timezone.utc).replace(tzinfo=None)

            # Gelecek tarih kontrolü
            if best_dt > today + timedelta(days=MAX_FUTURE_DAYS):
                logger.warning(f"      Future date rejected: {best_date}")
                best_date = real_date if real_date != "unknown" else today.strftime("%Y-%m-%d")

            # Çok eski tarih kontrolü (MIN_VALID_EVENT_YEAR öncesi)
            if best_dt.year < MIN_VALID_EVENT_YEAR:
                logger.warning(
                    f"      OLD DATE detected ({best_date}), "
                    f"marking stale: {result.get('summary_en', '')[:60]}"
                )
                result["is_stale"] = True
                return result
        except (ValueError, TypeError):
            pass

        # ── Google News URL kontrolü — çözülememiş link varsa tarih güvenilirliğini düşür ──
        source_url = url or ""
        if "news.google.com" in source_url and real_date == "unknown":
            # Google News linki çözülemediyse ve gerçek tarih yoksa,
            # sadece RSS tarihine güveniyoruz — bu riskli
            logger.warning(
                f"      Unresolved Google News URL + no real date — lower confidence"
            )
            result["data_quality"] = "low"

        result["date"] = best_date
        result["event_date"] = event_date if event_date != "unknown" else best_date
        result["publish_date"] = real_date if real_date != "unknown" else (publish_date if publish_date != "unknown" else article_pub)
        result["year"] = int(best_date[:4]) if best_date and best_date != "unknown" else None

        # Kaynak URL ekle
        result["source_urls"] = [url] if url else []
        result["source_articles"] = [0]

        # Varsayılan alanlar
        # incident_type normalleştirme (LLM bazen "A"/"B" döndürüyor)
        raw_type = result.get("incident_type", category)
        result["incident_type"] = normalize_incident_type(raw_type)
        result.setdefault("country", "unknown")
        result.setdefault("city", "unknown")
        result.setdefault("severity", "medium")
        result.setdefault("data_quality", "medium")
        result.setdefault("casualties_dead", 0)
        result.setdefault("casualties_injured", 0)
        result.setdefault("geo_lat", None)
        result.setdefault("geo_lon", None)
        result.setdefault("summary_tr", "")
        result.setdefault("summary_en", "")
        result.setdefault("is_false_alarm", False)

        # False alarm ise severity düşür
        if result.get("is_false_alarm"):
            result["severity"] = "low"
            tags = result.get("tags", [])
            if "false_alarm" not in tags:
                tags.append("false_alarm")
            result["tags"] = tags
            
        # Sayısal alanları int'e zorla
        for nf in ["casualties_dead", "casualties_injured"]:
            try:
                result[nf] = int(result[nf] or 0)
            except (ValueError, TypeError):
                result[nf] = 0

        # Boolean alanları düzelt (LLM bazen string döndürür)
        for bf in ["is_false_alarm", "is_ongoing"]:
            val = result.get(bf)
            if isinstance(val, str):
                result[bf] = val.lower() in ("true", "1", "yes")

        # attack_type'ı normalize et
        attack_map = {
            "hava saldırısı": "airstrike",
            "bombalama": "bombing",
            "bombalı saldırı": "bombing",
            "silahlı saldırı": "shooting",
            "bıçaklı saldırı": "stabbing",
            "drone saldırısı": "drone",
            "intihar saldırısı": "bombing",
            "araçlı saldırı": "vehicle",
            "roket saldırısı": "bombing",
            "füze saldırısı": "bombing",
            "airstrike": "airstrike",
            "air strike": "airstrike",
            "rocket attack": "bombing",
            "missile": "bombing",
            "car bomb": "bombing",
            "suicide bomb": "bombing",
            "ied": "bombing",
        }
        at = (result.get("attack_type") or "other").lower()
        result["attack_type"] = attack_map.get(at, at)

        valid_attacks = {
            "bombing", "shooting", "stabbing", "assault", "explosion",
            "siege", "threat", "drone", "arson", "airstrike",
            "vehicle", "other"
        }
        if result["attack_type"] not in valid_attacks:
            result["attack_type"] = "other"

        # null string'leri None'a çevir
        for field in ["airport_name", "airport_iata", "hotel_name"]:
            if result.get(field) in ("null", "NULL", "None", "none", ""):
                result[field] = None

        # Ülke boşsa şehirden çıkar
        known_cities = {
            "kerkük": ("Iraq", "IQ"), "kirkuk": ("Iraq", "IQ"),
            "bağdat": ("Iraq", "IQ"), "baghdad": ("Iraq", "IQ"),
            "erbil": ("Iraq", "IQ"), "basra": ("Iraq", "IQ"),
            "mosul": ("Iraq", "IQ"), "dubai": ("United Arab Emirates", "AE"),
            "abu dhabi": ("United Arab Emirates", "AE"),
            "riyadh": ("Saudi Arabia", "SA"), "jeddah": ("Saudi Arabia", "SA"),
            "doha": ("Qatar", "QA"), "kuwait city": ("Kuwait", "KW"),
            "beirut": ("Lebanon", "LB"), "damascus": ("Syria", "SY"),
            "aleppo": ("Syria", "SY"), "kabul": ("Afghanistan", "AF"),
            "mogadishu": ("Somalia", "SO"), "istanbul": ("Turkey", "TR"),
            "ankara": ("Turkey", "TR"), "tel aviv": ("Israel", "IL"),
            "jerusalem": ("Israel", "IL"), "tehran": ("Iran", "IR"),
            "cairo": ("Egypt", "EG"), "tripoli": ("Libya", "LY"),
            "sanaa": ("Yemen", "YE"), "aden": ("Yemen", "YE"),
        }
        if result.get("country") in ("unknown", "null", "", None):
            city_lower = (result.get("city") or "").lower().strip()
            if city_lower in known_cities:
                result["country"] = known_cities[city_lower][0]
                result["country_code"] = known_cities[city_lower][1]

        return result

    # ═══════════════════════════════════════
    # ÇOKLU SİNYAL DOĞRULAMA
    # ═══════════════════════════════════════

    def _validate_incident(self, incident: Dict, source_article: Dict) -> Dict:
        """
        LLM confidence yerine deterministik sinyaller ile doğrulama.
        Her sinyal 0 veya 1, toplam skor 0.0 - 1.0 arası.
        """

        signals = {}
        title = source_article.get("title", "").lower()
        summary = source_article.get("summary", "").lower()
        source_text = f"{title} {summary}"

        # ── 1. Ülke bilgisi var mı? ──
        country = (incident.get("country") or "").lower()
        signals["has_country"] = (
            country not in ("unknown", "null", "", "none")
            and len(country) > 2
        )

        # ── 2. Şehir bilgisi var mı? ──
        city = (incident.get("city") or "").lower()
        signals["has_city"] = (
            city not in ("unknown", "null", "", "none")
            and len(city) > 2
        )

        # ── 3. Tarih geçerli mi? ──
        date = incident.get("date", "")
        signals["has_valid_date"] = bool(
            date and re.match(r"^\d{4}-\d{2}-\d{2}$", date)
        )

        # ── 4. Özet kaynak metinle örtüşüyor mu? ──
        summary_en = (incident.get("summary_en") or "").lower()
        if summary_en and source_text:
            summary_words = set(re.findall(r"[a-z]{4,}", summary_en))
            source_words = set(re.findall(r"[a-z]{4,}", source_text))
            if summary_words:
                overlap = len(summary_words & source_words) / len(summary_words)
                signals["text_overlap"] = overlap > 0.3
            else:
                signals["text_overlap"] = False
        else:
            signals["text_overlap"] = False

        # ── 5. Olay tipi geçerli mi? ──
        valid_types = {
            "AIRPORT_ATTACK", "AIRLINE_PERSONNEL", "HOTEL_ATTACK"
        }
        signals["valid_type"] = incident.get("incident_type") in valid_types

        # ── 6. Saldırı tipi kaynak metinde var mı? ──
        attack_type = (incident.get("attack_type") or "").lower()
        attack_keywords = {
            "bombing": ["bomb", "bombing", "explosive"],
            "shooting": ["shoot", "shot", "gun", "fire"],
            "stabbing": ["stab", "knife", "blade"],
            "assault": ["assault", "punch", "attack", "hit"],
            "explosion": ["explo", "blast", "detonate"],
            "siege": ["siege", "hostage", "storm"],
            "threat": ["threat", "threaten", "bomb threat"],
            "drone": ["drone", "uav", "unmanned"],
            "arson": ["arson", "fire", "set fire"],
            "other": [],
        }
        related_words = attack_keywords.get(attack_type, [])
        signals["attack_in_source"] = (
            any(w in source_text for w in related_words)
            if related_words else True
        )

        # ── 7. Konum kaynak metinde var mı? ──
        location_found = False
        for field in ["country", "city", "airport_name", "hotel_name"]:
            val = (incident.get(field) or "").lower()
            if val and val not in ("unknown", "null", "") and val in source_text:
                location_found = True
                break
        signals["location_in_source"] = location_found

        # ── 8. False alarm mı? ──
        signals["not_false_alarm"] = not incident.get("is_false_alarm", False)

        # ── SKOR HESAPLA ──
        weights = {
            "has_country": 0.15,
            "has_city": 0.10,
            "has_valid_date": 0.10,
            "text_overlap": 0.20,
            "valid_type": 0.10,
            "attack_in_source": 0.15,
            "location_in_source": 0.15,
            "not_false_alarm": 0.05,
        }

        score = sum(
            weights[k] for k, v in signals.items() if v
        )

        # Status belirleme
        if score >= 0.7:
            status = "verified"
        elif score >= 0.5:
            status = "probable"
        elif score >= 0.3:
            status = "unverified"
        else:
            status = "rejected"

        return {
            "score": round(score, 2),
            "status": status,
            "signals": {k: v for k, v in signals.items()},
        }
    def _normalize_date(self, date_str: str) -> str:
        """Çeşitli tarih formatlarını YYYY-MM-DD'ye çevir."""
        if not date_str or date_str in ("unknown", "null", "", "None"):
            return "unknown"

        if re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
            return date_str

        m = re.match(r"^(\d{4})(\d{2})(\d{2})", date_str)
        if m:
            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

        try:
            from dateutil import parser as dp
            dt = dp.parse(date_str)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            pass

        return "unknown"
        
