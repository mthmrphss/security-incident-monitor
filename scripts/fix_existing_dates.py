#!/usr/bin/env python3
"""
One-time script: Mevcut incidents.json'daki kayıtların tarihlerini düzeltir.

Her kaydın source_urls listesindeki doğrudan haber URL'lerine gidip
HTML meta tag'lerinden gerçek yayınlanma tarihini çıkarır ve
publish_date / date alanlarını günceller.

Kullanım:
    python scripts/fix_existing_dates.py

Güvenli: Önce yedek alır (data/incidents_backup.json)
"""

import os
import sys
import json
import re
import time
import logging
from datetime import datetime, timezone
from copy import deepcopy

import requests
from bs4 import BeautifulSoup
from dateutil import parser as date_parser

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("DateFixer")

USER_AGENT = "SecurityIncidentMonitor/2.0 (date-fix-script)"

# ─────────────────────────────────────
# TARİH ÇIKARIM FONKSİYONLARI
# (collectors.py ile aynı mantık)
# ─────────────────────────────────────

def try_parse_date(date_str: str) -> str:
    """Tarih string'ini YYYY-MM-DD formatına çevir."""
    if not date_str or len(date_str) < 8:
        return ""
    try:
        dt = date_parser.parse(date_str)
        if dt.year < 2000 or dt.year > 2030:
            return ""
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return ""


def extract_publish_date(soup: BeautifulSoup) -> str:
    """HTML sayfasından gerçek yayınlanma tarihini çıkar."""

    # 1. article:published_time meta tag
    for attr in ["article:published_time", "og:article:published_time"]:
        tag = soup.find("meta", attrs={"property": attr})
        if tag and tag.get("content"):
            parsed = try_parse_date(tag["content"])
            if parsed:
                return parsed

    # 2. name-based meta tags
    for name in ["date", "publish_date", "pubdate", "publishdate",
                  "article_date", "article:date", "DC.date.issued"]:
        tag = soup.find("meta", attrs={"name": re.compile(name, re.I)})
        if tag and tag.get("content"):
            parsed = try_parse_date(tag["content"])
            if parsed:
                return parsed

    # 3. <time> element
    time_tag = soup.find("time", attrs={"datetime": True})
    if time_tag:
        parsed = try_parse_date(time_tag["datetime"])
        if parsed:
            return parsed

    # 4. JSON-LD
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            ld_data = json.loads(script.string or "")
            items = ld_data if isinstance(ld_data, list) else [ld_data]
            for item in items:
                dp = item.get("datePublished")
                if dp:
                    parsed = try_parse_date(dp)
                    if parsed:
                        return parsed
                for graph_item in item.get("@graph", []):
                    dp = graph_item.get("datePublished")
                    if dp:
                        parsed = try_parse_date(dp)
                        if parsed:
                            return parsed
        except (json.JSONDecodeError, AttributeError, TypeError):
            continue

    return ""


def fetch_real_date(url: str, session: requests.Session) -> str:
    """Bir URL'den gerçek yayınlanma tarihini çıkar."""
    try:
        resp = session.get(url, timeout=10, allow_redirects=True)
        if resp.status_code != 200:
            return ""
        content_type = resp.headers.get("content-type", "")
        if "text/html" not in content_type:
            return ""
        soup = BeautifulSoup(resp.text, "lxml")
        return extract_publish_date(soup)
    except Exception as e:
        logger.warning(f"  Fetch error: {str(e)[:60]}")
        return ""


# ─────────────────────────────────────
# ANA DÜZELTME MANTIĞI
# ─────────────────────────────────────

def main():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    incidents_file = os.path.join(root, "data", "incidents.json")
    backup_file = os.path.join(root, "data", "incidents_backup.json")

    # 1. Veriyi yükle
    with open(incidents_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    incidents = data["incidents"]
    logger.info(f"Toplam kayıt: {len(incidents)}")

    # 2. Yedek al
    with open(backup_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info(f"Yedek alındı: {backup_file}")

    # 3. Session oluştur
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    # 4. İstatistikler
    fixed = 0
    skipped_google = 0
    skipped_no_date = 0
    already_ok = 0
    errors = 0

    for i, inc in enumerate(incidents):
        inc_id = inc.get("id", "?")[:8]
        city = inc.get("city", "?")
        old_date = inc.get("date", "?")
        old_publish = inc.get("publish_date", "?")

        # Doğrudan URL'leri al (Google News hariç)
        source_urls = inc.get("source_urls", [])
        direct_urls = [u for u in source_urls if "news.google.com" not in u]

        if not direct_urls:
            skipped_google += 1
            continue

        # İlk erişilebilir URL'den tarih çıkar
        real_date = ""
        for url in direct_urls:
            real_date = fetch_real_date(url, session)
            if real_date:
                break
            time.sleep(0.3)

        if not real_date:
            skipped_no_date += 1
            logger.info(f"  [{i+1:2d}] {inc_id} {city:20s} | Tarih çıkarılamadı")
            continue

        # Karşılaştır
        if real_date == old_date and real_date == old_publish:
            already_ok += 1
            continue

        # Güncelle
        logger.info(
            f"  [{i+1:2d}] {inc_id} {city:20s} | "
            f"date: {old_date} → {real_date} | "
            f"publish: {old_publish} → {real_date}"
        )

        inc["real_publish_date"] = real_date
        inc["publish_date"] = real_date

        # date alanı: event_date geçerliyse onu koru, değilse real_date kullan
        event_date = inc.get("event_date", "")
        parsed_event = try_parse_date(str(event_date)) if event_date else ""

        if parsed_event and parsed_event != real_date:
            # Gerçek bir olay tarihi var (Mumbai 2008 gibi) — koru ama date'i publish'e eşitle
            inc["date"] = real_date
            logger.info(f"         event_date korundu: {parsed_event}")
        else:
            inc["date"] = real_date

        # year alanını güncelle
        try:
            inc["year"] = int(inc["date"][:4])
        except (ValueError, TypeError):
            pass

        inc["last_updated"] = datetime.now(timezone.utc).isoformat()
        fixed += 1

        # Rate limiting
        time.sleep(0.5)

    # 5. Kaydet
    data["incidents"] = incidents
    data["metadata"]["last_updated"] = datetime.now(timezone.utc).isoformat()

    with open(incidents_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    # 6. Özet
    logger.info("=" * 50)
    logger.info("ÖZET")
    logger.info(f"  Düzeltilen  : {fixed}")
    logger.info(f"  Zaten doğru : {already_ok}")
    logger.info(f"  Google-only : {skipped_google} (URL çözülemedi)")
    logger.info(f"  Tarih yok   : {skipped_no_date}")
    logger.info(f"  Toplam      : {len(incidents)}")
    logger.info("=" * 50)


if __name__ == "__main__":
    main()
