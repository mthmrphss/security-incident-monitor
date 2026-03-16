# scripts/analyzer.py

import json
import time
import logging
from typing import List, Dict

import google.generativeai as genai

logger = logging.getLogger("Analyzer")


class GeminiAnalyzer:

    def __init__(self, api_key: str):
        genai.configure(api_key=api_key)

        self.model = genai.GenerativeModel(
            model_name="gemini-2.0-flash",
            generation_config={
                "temperature": 0.05,
                "top_p": 0.9,
                "max_output_tokens": 8192,
                "response_mime_type": "application/json",
            },
        )

        self.validator = genai.GenerativeModel(
            model_name="gemini-2.0-flash",
            generation_config={
                "temperature": 0.0,
                "max_output_tokens": 4096,
                "response_mime_type": "application/json",
            },
        )

        # ── RATE LIMIT AYARLARI ──
        self._last_request_time = 0
        self._min_interval = 6       # İstekler arası minimum 6 saniye
        self._retry_base_wait = 30   # 429 hatası sonrası bekleme (saniye)
        self._max_retries = 4        # Maksimum deneme

    # ═══════════════════════════════════════
    # RATE LIMITER
    # ═══════════════════════════════════════

    def _wait_for_rate_limit(self):
        """İstekler arası minimum süreyi garantile."""
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < self._min_interval:
            wait = self._min_interval - elapsed
            logger.info(f"      ⏳ Rate limit: {wait:.1f}s bekleniyor...")
            time.sleep(wait)
        self._last_request_time = time.time()

    def _safe_generate(self, model, prompt: str, label: str = "") -> dict:
        """
        Rate-limit korumalı API çağrısı.
        429 hatalarında otomatik bekleyip tekrar dener.
        """
        for attempt in range(self._max_retries):
            try:
                self._wait_for_rate_limit()

                logger.info(f"      🤖 API call: {label} (attempt {attempt+1})")
                response = model.generate_content(prompt)

                if not response.text:
                    logger.warning(f"      Empty response for {label}")
                    return {}

                self._last_request_time = time.time()
                return json.loads(response.text)

            except Exception as e:
                error_str = str(e)

                if "429" in error_str or "quota" in error_str.lower() or "rate" in error_str.lower():
                    # ── RATE LIMIT HATASI ──
                    # Hata mesajından bekleme süresini çıkarmaya çalış
                    wait = self._retry_base_wait * (attempt + 1)

                    # "retry in XX.XXs" varsa onu kullan
                    import re
                    retry_match = re.search(r"retry in (\d+\.?\d*)", error_str.lower())
                    if retry_match:
                        wait = max(float(retry_match.group(1)) + 5, wait)

                    logger.warning(
                        f"      ⚠️ Rate limit! {wait:.0f}s bekleniyor... "
                        f"(attempt {attempt+1}/{self._max_retries})"
                    )
                    time.sleep(wait)

                elif "json" in error_str.lower() or "parse" in error_str.lower():
                    logger.warning(f"      JSON parse error: {e}")
                    if attempt < self._max_retries - 1:
                        time.sleep(10)

                else:
                    logger.error(f"      API error: {e}")
                    if attempt < self._max_retries - 1:
                        time.sleep(15)

        logger.error(f"      ❌ {label}: All {self._max_retries} attempts failed")
        return {}

    # ═══════════════════════════════════════
    # MAIN PIPELINE
    # ═══════════════════════════════════════

    def analyze_batch(self, articles: List[Dict]) -> List[Dict]:
        logger.info("    Stage 1: Classification...")
        relevant = self._stage1_classify(articles)
        logger.info(f"      {len(relevant)}/{len(articles)} relevant")

        if not relevant:
            return []

        logger.info("    Stage 2: Extraction...")
        incidents = self._stage2_extract(relevant)
        logger.info(f"      {len(incidents)} incidents extracted")

        if not incidents:
            return []

        logger.info("    Stage 3: Verification...")
        verified = self._stage3_verify(incidents, relevant)
        logger.info(f"      {len(verified)} verified")

        return verified

    # ═══════════════════════════════════════
    # STAGE 1 — CLASSIFY
    # ═══════════════════════════════════════

    def _stage1_classify(self, articles: List[Dict]) -> List[Dict]:
        articles_text = ""
        for i, art in enumerate(articles):
            articles_text += (
                f"[{i}] {art.get('title', '')}\n"
                f"    {art.get('summary', '')[:300]}\n\n"
            )

        prompt = f"""You are a security news classifier.

For EACH article below, determine if it belongs to one of these categories:

A) AIRPORT_ATTACK — Physical attack on an airport (bomb, gun, knife, vehicle, explosion, terror)
B) AIRLINE_PERSONNEL — Attack on airline workers (cabin crew, pilot, ground staff being assaulted/threatened by passengers or others)
C) HOTEL_ATTACK — Physical attack on a hotel (bomb, armed raid, hostage, explosion, terror)

NOT RELEVANT — FILTER OUT:
- Airport strikes, cancellations, delays
- Routine security checks or screenings
- Aircraft accidents, technical failures
- Hotel fires (unless arson attack)
- Hotel theft, fraud
- Movies, books, documentaries about attacks
- Anniversary/memorial articles about PAST events
- Security technology/product news
- Court rulings about old cases
- Travel tips, reviews, tourism statistics

FEW-SHOT EXAMPLES:

"Gunman opens fire at Istanbul airport, 3 dead"
→ relevant: true, category: "AIRPORT_ATTACK", confidence: 0.98

"Drunk passenger punches flight attendant on Delta flight"
→ relevant: true, category: "AIRLINE_PERSONNEL", confidence: 0.92

"Al-Shabaab storms beachside hotel in Mogadishu"
→ relevant: true, category: "HOTEL_ATTACK", confidence: 0.97

"New airport security scanners reduce wait times"
→ relevant: false, reason: "security tech news, not an attack"

"Airport workers strike over pay in France"
→ relevant: false, reason: "labor strike, not an attack"

"Anniversary of 2016 Istanbul airport attack"
→ relevant: false, reason: "memorial of past event"

ARTICLES TO CLASSIFY:

{articles_text}

Return JSON:
{{
  "classifications": [
    {{
      "index": 0,
      "relevant": true,
      "category": "AIRPORT_ATTACK",
      "confidence": 0.95,
      "reason": "why relevant or not"
    }}
  ]
}}

If confidence < 0.7, set relevant to false."""

        result = self._safe_generate(self.model, prompt, "Stage1-Classify")

        if not result:
            return articles  # Fallback: hepsini geçir

        relevant = []
        for cl in result.get("classifications", []):
            idx = cl.get("index", -1)
            if (
                cl.get("relevant") is True
                and cl.get("confidence", 0) >= 0.7
                and 0 <= idx < len(articles)
            ):
                art = articles[idx].copy()
                art["ai_category"] = cl.get("category")
                art["ai_confidence"] = cl.get("confidence")
                art["ai_reason"] = cl.get("reason", "")
                relevant.append(art)

        return relevant

    # ═══════════════════════════════════════
    # STAGE 2 — EXTRACT
    # ═══════════════════════════════════════

    def _stage2_extract(self, articles: List[Dict]) -> List[Dict]:
        articles_text = ""
        for i, art in enumerate(articles):
            articles_text += (
                f"[{i}] Source: {art.get('source', 'N/A')}\n"
                f"    Category: {art.get('ai_category', 'N/A')}\n"
                f"    Date: {art.get('published', 'N/A')}\n"
                f"    Title: {art.get('title', '')}\n"
                f"    Summary: {art.get('summary', '')[:500]}\n"
                f"    URL: {art.get('url', '')}\n\n"
            )

        prompt = f"""Extract security incident details from these articles.

CRITICAL RULES:
1. Only write information EXPLICITLY stated in the article. Do NOT guess.
2. Use "unknown" for missing information. Do NOT invent.
3. If multiple articles describe the SAME event, merge into ONE incident.
4. Date format: YYYY-MM-DD.
5. Country names in English.
6. Summary max 200 characters.

ARTICLES:

{articles_text}

OUTPUT FORMAT:
{{
  "incidents": [
    {{
      "incident_type": "AIRPORT_ATTACK|AIRLINE_PERSONNEL|HOTEL_ATTACK",
      "date": "YYYY-MM-DD",
      "year": 2024,
      "country": "Country (English)",
      "country_code": "XX",
      "city": "City",
      "location_detail": "Specific location",
      "airport_name": "Airport name or null",
      "airport_iata": "XXX or null",
      "hotel_name": "Hotel name or null",
      "venue_name": "General venue name",
      "attack_type": "bombing|shooting|stabbing|vehicle|assault|explosion|siege|threat|arson|other",
      "severity": "critical|high|medium|low",
      "casualties_dead": 0,
      "casualties_injured": 0,
      "perpetrator": "Who did it or unknown",
      "summary_tr": "Turkish summary max 200 chars",
      "summary_en": "English summary max 200 chars",
      "source_urls": ["url1"],
      "source_articles": [0],
      "is_confirmed": true,
      "data_quality": "high|medium|low",
      "tags": ["tag1", "tag2"]
    }}
  ]
}}

If NO valid incidents found, return: {{"incidents": []}}"""

        result = self._safe_generate(self.model, prompt, "Stage2-Extract")

        if not result:
            return []

        incidents = result.get("incidents", [])

        for inc in incidents:
            src_indices = inc.get("source_articles", [])
            urls = []
            for idx in src_indices:
                if 0 <= idx < len(articles):
                    u = articles[idx].get("url", "")
                    if u:
                        urls.append(u)
            if urls:
                inc["source_urls"] = urls
            elif not inc.get("source_urls"):
                inc["source_urls"] = []

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

        return incidents

    # ═══════════════════════════════════════
    # STAGE 3 — VERIFY
    # ═══════════════════════════════════════

    def _stage3_verify(self, incidents: List[Dict], source_articles: List[Dict]) -> List[Dict]:
        if not incidents:
            return []

        verified = []

        for inc_index, inc in enumerate(incidents):
            related_texts = []
            for idx in inc.get("source_articles", []):
                if 0 <= idx < len(source_articles):
                    art = source_articles[idx]
                    related_texts.append(
                        f"Title: {art.get('title', '')}\n"
                        f"Summary: {art.get('summary', '')[:400]}"
                    )

            if not related_texts:
                for art in source_articles:
                    title_lower = art.get("title", "").lower()
                    summary_en = inc.get("summary_en", "").lower()
                    common = set(summary_en.split()) & set(title_lower.split())
                    if len(common) >= 3:
                        related_texts.append(
                            f"Title: {art.get('title', '')}\n"
                            f"Summary: {art.get('summary', '')[:400]}"
                        )

            if not related_texts:
                logger.warning(f"      No source for: {inc.get('summary_en', '')[:50]}")
                continue

            sources_text = "\n---\n".join(related_texts[:3])

            prompt = f"""Compare the extracted incident with source articles.

EXTRACTED INCIDENT:
{json.dumps(inc, ensure_ascii=False, indent=2)}

SOURCE ARTICLES:
{sources_text}

CHECK:
1. Is this actually a physical ATTACK/ASSAULT?
2. Is the country/city in the source?
3. Is the date consistent?
4. Are casualty numbers supported?
5. Is this a CURRENT event?

Return JSON:
{{
  "verdict": "ACCEPT|FIX|REJECT",
  "confidence": 0.0-1.0,
  "issues": ["issue descriptions"],
  "corrections": {{"field": "corrected_value"}},
  "reason": "explanation"
}}"""

            validation = self._safe_generate(
                self.validator, prompt, f"Stage3-Verify-{inc_index}"
            )

            if not validation:
                if inc.get("data_quality") == "high":
                    inc["verification_status"] = "unverified"
                    verified.append(inc)
                continue

            verdict = validation.get("verdict", "REJECT")
            conf = validation.get("confidence", 0)

            if verdict == "ACCEPT" and conf >= 0.6:
                inc["verification_score"] = conf
                inc["verification_status"] = "verified"
                verified.append(inc)
                logger.info(f"      ACCEPT ({conf:.0%}): {inc.get('summary_en', '')[:50]}")

            elif verdict == "FIX" and conf >= 0.5:
                for field, value in validation.get("corrections", {}).items():
                    if field in inc and value:
                        inc[field] = value
                inc["verification_score"] = conf
                inc["verification_status"] = "corrected"
                verified.append(inc)
                logger.info(f"      FIX ({conf:.0%}): {inc.get('summary_en', '')[:50]}")

            else:
                reason = validation.get("reason", "unknown")
                logger.info(f"      REJECT ({conf:.0%}): {inc.get('summary_en', '')[:50]} — {reason}")

        return verified
