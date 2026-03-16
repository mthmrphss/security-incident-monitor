# scripts/analyzer.py

import json
import time
import re
import logging
from typing import List, Dict

import google.generativeai as genai

logger = logging.getLogger("Analyzer")


class GeminiAnalyzer:

    def __init__(self, api_key: str):
        genai.configure(api_key=api_key)

        # ── Birden fazla model — biri doluysa diğerini dene ──
        self._model_names = [
            "gemini-2.5-flash-lite",    # 4K RPM, Unlimited RPD, en ucuz
            "gemini-2.0-flash",         # 2K RPM, Unlimited RPD, yedek
        ]

        self._models = {}
        for name in self._model_names:
            try:
                self._models[name] = genai.GenerativeModel(
                    model_name=name,
                    generation_config={
                        "temperature": 0.05,
                        "top_p": 0.9,
                        "max_output_tokens": 4096,
                        "response_mime_type": "application/json",
                    },
                )
            except Exception as e:
                logger.warning(f"Model {name} init failed: {e}")

        self._last_request_time = 0
        self._min_interval = 2
        self._max_retries = 3

    # ═══════════════════════════════════════
    # RATE LIMITER
    # ═══════════════════════════════════════

    def _wait(self):
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < self._min_interval:
            wait = self._min_interval - elapsed
            time.sleep(wait)
        self._last_request_time = time.time()

    def _call_api(self, prompt: str, label: str = "") -> dict:
        """
        Tüm modelleri sırayla dene.
        Biri 429 verirse sonrakine geç.
        """
        for model_name in self._model_names:
            model = self._models.get(model_name)
            if not model:
                continue

            for attempt in range(self._max_retries):
                try:
                    self._wait()
                    logger.info(f"      API: {model_name} (try {attempt+1})")

                    response = model.generate_content(prompt)

                    if not response.text:
                        logger.warning(f"      Empty response from {model_name}")
                        continue

                    self._last_request_time = time.time()
                    return json.loads(response.text)

                except Exception as e:
                    err = str(e).lower()

                    if "429" in str(e) or "quota" in err or "rate" in err:
                        # Bu modelin kotası dolmuş
                        retry_match = re.search(r"retry in (\d+\.?\d*)", err)
                        if retry_match:
                            wait = float(retry_match.group(1)) + 5
                        else:
                            wait = 20 * (attempt + 1)

                        logger.warning(
                            f"      {model_name} rate limited, "
                            f"waiting {wait:.0f}s (try {attempt+1})"
                        )
                        time.sleep(wait)

                        # Son denemeyse bu modeli bırak, sonrakine geç
                        if attempt == self._max_retries - 1:
                            logger.warning(
                                f"      {model_name} exhausted, trying next model..."
                            )
                    else:
                        logger.warning(f"      {model_name} error: {e}")
                        if attempt < self._max_retries - 1:
                            time.sleep(10)

        logger.error(f"      ALL models failed for: {label}")
        return {}

    # ═══════════════════════════════════════
    # TEK AŞAMALI ANALİZ (3-in-1)
    # ═══════════════════════════════════════

    def analyze_batch(self, articles: List[Dict]) -> List[Dict]:
        """
        Tek API çağrısıyla:
          1. İlgili mi? (sınıflandırma)
          2. Detayları çıkar
          3. Tutarlılık kontrolü
        """

        # Makale metinlerini hazırla — KISA tut (token tasarrufu)
        articles_text = ""
        for i, art in enumerate(articles):
            title = art.get("title", "")[:150]
            summary = art.get("summary", "")[:200]
            pub_date = art.get("published", "")[:10]
            articles_text += f"[{i}] DATE:{pub_date} | {title}\n{summary}\n\n"

        prompt = f"""Analyze these news articles. Find ONLY real physical security incidents in these categories:

A) AIRPORT_ATTACK — Physical attack on airport (bomb, gun, knife, explosion, terror)
B) AIRLINE_PERSONNEL — Physical attack on airline workers by passengers or others  
C) HOTEL_ATTACK — Physical attack on hotel (bomb, armed raid, hostage, terror)

IGNORE: strikes, delays, accidents, tech news, old events, memorials, reviews, theft, fraud, fires (unless arson)

ARTICLES:
{articles_text}

For each REAL incident found, return:
{{
  "incidents": [
    {{
      "incident_type": "AIRPORT_ATTACK|AIRLINE_PERSONNEL|HOTEL_ATTACK",
      "date": "YYYY-MM-DD",
      "year": 2025,
      "country": "Country English",
      "country_code": "XX",
      "city": "City",
      "location_detail": "where exactly",
      "airport_name": "name or null",
      "airport_iata": "XXX or null",
      "hotel_name": "name or null",
      "venue_name": "general name",
      "attack_type": "bombing|shooting|stabbing|assault|explosion|siege|threat|arson|other",
      "severity": "critical|high|medium|low",
      "casualties_dead": 0,
      "casualties_injured": 0,
      "perpetrator": "who or unknown",
      "summary_tr": "Turkce ozet max 150 chars",
      "summary_en": "English summary max 150 chars",
      "source_articles": [0],
      "confidence": 0.95,
      "data_quality": "high|medium|low",
      "tags": ["tag1"]
    }}
  ]
}}

RULES:
- ONLY info explicitly in the article. Do NOT invent.
- Use "unknown" for missing info EXCEPT date.
- For DATE: use the article's DATE field shown above. NEVER return "unknown" for date.
- Merge same event from multiple articles into ONE single incident.
- SAME airport + SAME country = SAME event, merge them.
- SAME hotel + SAME country = SAME event, merge them.
- If NO real incidents, return {{"incidents": []}}
- confidence must be >= 0.7"""

        logger.info(f"    Analyzing {len(articles)} articles (single-stage)...")

        result = self._call_api(prompt, "Analyze")

        if not result:
            return []

        incidents = result.get("incidents", [])

        # Sonuçları temizle
        valid = []
        for inc in incidents:
            # Düşük güvenli olanları at
            if inc.get("confidence", 0) < 0.7:
                logger.info(
                    f"      SKIP (low confidence {inc.get('confidence', 0):.0%}): "
                    f"{inc.get('summary_en', '')[:50]}"
                )
                continue

            # Kaynak URL'leri ekle
            src_indices = inc.get("source_articles", [])
            urls = []
            for idx in src_indices:
                if 0 <= idx < len(articles):
                    u = articles[idx].get("url", "")
                    if u:
                        urls.append(u)
            inc["source_urls"] = urls if urls else []

            # Varsayılan alanlar
            inc.setdefault("incident_type", "UNKNOWN")
            inc.setdefault("date", "unknown")
            inc.setdefault("country", "unknown")
            inc.setdefault("city", "unknown")
            inc.setdefault("severity", "medium")
            inc.setdefault("data_quality", "medium")
            inc.setdefault("casualties_dead", 0)
            inc.setdefault("casualties_injured", 0)
            inc.setdefault("geo_lat", None)
            inc.setdefault("geo_lon", None)
            inc.setdefault("summary_tr", "")
            inc.setdefault("summary_en", "")
            inc["verification_status"] = "auto"
            inc["verification_score"] = inc.get("confidence", 0.7)

            valid.append(inc)
            logger.info(
                f"      ✅ {inc.get('confidence', 0):.0%} "
                f"{inc.get('incident_type', '?')}: "
                f"{inc.get('summary_en', '')[:60]}"
            )

        logger.info(f"    Result: {len(valid)} verified incidents")
        return valid
