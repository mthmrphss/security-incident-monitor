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
- Hijacking (unless physical crew assault)
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
→ relevant: false, reason: "memorial of past event, not new incident"

"Hotel & Resort industry reports record tourism"
→ relevant: false, reason: "tourism statistics"

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

        try:
            response = self.model.generate_content(prompt)
            result = json.loads(response.text)

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

        except Exception as e:
            logger.error(f"Stage 1 error: {e}")
            return articles

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
1. Only write information EXPLICITLY stated in the article. Do NOT guess or fabricate.
2. Use "unknown" for missing information. Do NOT invent locations, names, or numbers.
3. If multiple articles describe the SAME event, merge into ONE incident.
4. Date format: YYYY-MM-DD. If uncertain, use the article's publication date.
5. Country names in English.
6. Summary max 200 characters, clearly describe the event.

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

If multiple articles cover the SAME event, list all indices in source_articles and all URLs in source_urls.
If NO valid incidents found, return: {{"incidents": []}}"""

        try:
            time.sleep(2)
            response = self.model.generate_content(prompt)
            result = json.loads(response.text)
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

        except Exception as e:
            logger.error(f"Stage 2 error: {e}")
            return []

    # ═══════════════════════════════════════
    # STAGE 3 — VERIFY
    # ═══════════════════════════════════════

    def _stage3_verify(self, incidents: List[Dict], source_articles: List[Dict]) -> List[Dict]:
        if not incidents:
            return []

        verified = []

        for inc in incidents:
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
                logger.warning(f"      No source found for: {inc.get('summary_en', '')[:50]}")
                continue

            sources_text = "\n---\n".join(related_texts[:3])

            prompt = f"""Compare the extracted incident with the source articles.
Check if the extracted data is actually supported by the sources.

EXTRACTED INCIDENT:
{json.dumps(inc, ensure_ascii=False, indent=2)}

SOURCE ARTICLES:
{sources_text}

CHECK:
1. Is this actually a physical ATTACK/ASSAULT? (not accident, strike, or tech issue)
2. Is the country/city mentioned in the source?
3. Is the date consistent?
4. Are casualty numbers supported by the source?
5. Is this a CURRENT event (not a historical article or anniversary)?

Return JSON:
{{
  "verdict": "ACCEPT|FIX|REJECT",
  "confidence": 0.0-1.0,
  "issues": ["issue descriptions"],
  "corrections": {{"field": "corrected_value"}},
  "reason": "explanation"
}}

REJECT if: fabricated info, not an attack, historical article, or completely unsupported.
FIX if: mostly correct but some fields need correction.
ACCEPT if: all data is supported by sources."""

            try:
                time.sleep(1.5)
                response = self.validator.generate_content(prompt)
                validation = json.loads(response.text)

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

            except Exception as e:
                logger.warning(f"      Verify error: {e}")
                if inc.get("data_quality") == "high":
                    inc["verification_status"] = "unverified"
                    verified.append(inc)

        return verified