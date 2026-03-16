# scripts/collectors.py

import os
import re
import time
import hashlib
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict

import yaml
import feedparser
import requests
from bs4 import BeautifulSoup
from dateutil import parser as date_parser

logger = logging.getLogger("Collector")

USER_AGENT = "SecurityIncidentMonitor/2.0 (GitHub Actions Bot)"


class NewsCollector:

    def __init__(self, config_path: str):
        with open(config_path, "r", encoding="utf-8") as f:
            self.config = yaml.safe_load(f)

        self.keywords = []
        for lang, words in self.config.get("keywords", {}).items():
            self.keywords.extend([w.lower() for w in words])

        self.negative_keywords = [
            w.lower() for w in self.config.get("negative_keywords", [])
        ]

        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})

    # ══════════════════════════════════════
    # MAIN COLLECT
    # ══════════════════════════════════════

    def collect_all(self) -> List[Dict]:
        articles = []
        articles.extend(self._collect_rss())
        # articles.extend(self._collect_reddit())
        articles.extend(self._collect_gdelt())
        articles.extend(self._collect_nitter())

        seen = set()
        unique = []
        for a in articles:
            url = a.get("url", "")
            key = hashlib.md5(url.encode()).hexdigest() if url else hashlib.md5(
                a.get("title", str(time.time())).encode()
            ).hexdigest()
            if key not in seen:
                seen.add(key)
                a["url_hash"] = key
                unique.append(a)

        return unique

    # ══════════════════════════════════════
    # RSS
    # ══════════════════════════════════════

    def _collect_rss(self) -> List[Dict]:
        articles = []
        feeds = self.config.get("rss_feeds", [])

        for feed_cfg in feeds:
            for attempt in range(3):
                try:
                    logger.info(f"  RSS: {feed_cfg['name']}")
                    feed = feedparser.parse(feed_cfg["url"], agent=USER_AGENT)

                    if feed.bozo and not feed.entries:
                        if attempt < 2:
                            time.sleep(5 * (attempt + 1))
                            continue
                        break

                    for entry in feed.entries[:50]:
                        pub = self._parse_date(entry)
                        if not self._within_hours(pub, 72):
                            continue

                        articles.append({
                            "source": feed_cfg["name"],
                            "source_type": "rss",
                            "category": feed_cfg.get("category", "general"),
                            "title": entry.get("title", ""),
                            "summary": self._clean_html(
                                entry.get("summary", entry.get("description", ""))
                            ),
                            "url": entry.get("link", ""),
                            "published": pub,
                            "collected_at": datetime.now(timezone.utc).isoformat(),
                        })
                    break

                except Exception as e:
                    logger.warning(f"  RSS error ({feed_cfg['name']}): {e}")
                    if attempt < 2:
                        time.sleep(5 * (attempt + 1))

            time.sleep(1)

        logger.info(f"  RSS total: {len(articles)}")
        return articles

    # ══════════════════════════════════════
    # REDDIT
    # ══════════════════════════════════════

    def _collect_reddit(self) -> List[Dict]:
        articles = []
        subreddits = self.config.get("reddit", {}).get("subreddits", [])

        for sub_cfg in subreddits:
            sub_name = sub_cfg["name"]
            for query in sub_cfg.get("search_queries", []):
                try:
                    url = (
                        f"https://www.reddit.com/r/{sub_name}/search.json"
                        f"?q={requests.utils.quote(query)}"
                        f"&sort=new&restrict_sr=on&t=week&limit=15"
                    )
                    resp = self.session.get(url, timeout=15)

                    if resp.status_code == 429:
                        logger.warning("  Reddit rate limited, waiting 60s")
                        time.sleep(60)
                        resp = self.session.get(url, timeout=15)

                    if resp.status_code != 200:
                        continue

                    data = resp.json()
                    posts = data.get("data", {}).get("children", [])

                    for post in posts:
                        p = post.get("data", {})
                        articles.append({
                            "source": f"Reddit r/{sub_name}",
                            "source_type": "reddit",
                            "category": "general",
                            "title": p.get("title", ""),
                            "summary": (p.get("selftext", "") or "")[:1000],
                            "url": f"https://reddit.com{p.get('permalink', '')}",
                            "published": datetime.fromtimestamp(
                                p.get("created_utc", 0), tz=timezone.utc
                            ).isoformat(),
                            "collected_at": datetime.now(timezone.utc).isoformat(),
                            "score": p.get("score", 0),
                        })

                    time.sleep(2)
                except Exception as e:
                    logger.warning(f"  Reddit error (r/{sub_name}): {e}")

        logger.info(f"  Reddit total: {len(articles)}")
        return articles

    # ══════════════════════════════════════
    # GDELT
    # ══════════════════════════════════════

    def _collect_gdelt(self) -> List[Dict]:
        articles = []
        queries = [
            "airport attack", "airport bombing", "airport shooting",
            "hotel attack", "hotel bombing", "hotel siege",
            "airline crew assault", "flight attendant attacked",
            "airport terror", "hotel terror attack",
        ]

        for query in queries:
            for timespan in ["1d", "3d"]:
                try:
                    url = (
                        f"https://api.gdeltproject.org/api/v2/doc/doc"
                        f"?query={requests.utils.quote(query)}"
                        f"&mode=artlist&maxrecords=25&format=json"
                        f"&timespan={timespan}&sort=datedesc"
                    )
                    resp = self.session.get(url, timeout=20)
                    if resp.status_code == 200:
                        text = resp.text.strip()
                        if text and text.startswith("{"):
                            data = resp.json()
                            for art in data.get("articles", []):
                                articles.append({
                                    "source": "GDELT",
                                    "source_type": "gdelt",
                                    "category": "general",
                                    "title": art.get("title", ""),
                                    "summary": "",
                                    "url": art.get("url", ""),
                                    "published": art.get("seendate", ""),
                                    "collected_at": datetime.now(timezone.utc).isoformat(),
                                    "language": art.get("language", ""),
                                })
                    time.sleep(1.5)
                except Exception:
                    continue

        logger.info(f"  GDELT total: {len(articles)}")
        return articles

    # ══════════════════════════════════════
    # NITTER (Twitter/X)
    # ══════════════════════════════════════

    def _collect_nitter(self) -> List[Dict]:
        """
        Nitter RSS ile Twitter/X hesaplarından haber topla.

        - Birden fazla Nitter instance sırayla denenir
        - Her instance için max_retries kadar tekrar denenir
        - Çalışan instance hatırlanır, sonraki hesaplarda önce o denenir
        """
        articles = []
        nitter_cfg = self.config.get("nitter", {})

        instances = nitter_cfg.get("instances", ["https://nitter.net"])
        accounts = nitter_cfg.get("accounts", [])
        retry_delay = nitter_cfg.get("retry_delay", 5)
        max_retries = nitter_cfg.get("max_retries", 3)

        if not accounts:
            return articles

        logger.info(f"  Nitter: {len(accounts)} accounts, {len(instances)} instances")

        working_instance = None

        for account in accounts:
            handle = account["handle"]
            category = account.get("category", "general")
            fetched = False

            # Çalışan instance'ı öne al
            ordered = list(instances)
            if working_instance and working_instance in ordered:
                ordered.remove(working_instance)
                ordered.insert(0, working_instance)

            for instance_url in ordered:
                if fetched:
                    break

                rss_url = f"{instance_url}/{handle}/rss"

                for attempt in range(max_retries):
                    try:
                        logger.info(
                            f"    @{handle} via {instance_url} "
                            f"(try {attempt + 1}/{max_retries})"
                        )

                        resp = self.session.get(rss_url, timeout=12)

                        if resp.status_code != 200:
                            logger.warning(f"    HTTP {resp.status_code}")
                            if attempt < max_retries - 1:
                                time.sleep(retry_delay)
                            continue

                        content = resp.text
                        if not content or "<item>" not in content.lower():
                            logger.warning(f"    Empty/invalid response")
                            if attempt < max_retries - 1:
                                time.sleep(retry_delay)
                            continue

                        feed = feedparser.parse(content)

                        if not feed.entries:
                            logger.warning(f"    No entries")
                            if attempt < max_retries - 1:
                                time.sleep(retry_delay)
                            continue

                        # BAŞARILI
                        working_instance = instance_url
                        count = 0

                        for entry in feed.entries[:30]:
                            pub = self._parse_date(entry)
                            if not self._within_hours(pub, 72):
                                continue

                            raw_text = entry.get("title", "") or entry.get(
                                "description", ""
                            )
                            clean_text = self._clean_nitter_text(raw_text)

                            if not clean_text or len(clean_text) < 15:
                                continue

                            nitter_link = entry.get("link", "")
                            twitter_link = self._nitter_to_twitter_url(
                                nitter_link, instance_url
                            )

                            articles.append({
                                "source": f"Twitter @{handle}",
                                "source_type": "twitter",
                                "category": category,
                                "title": clean_text[:200],
                                "summary": clean_text,
                                "url": twitter_link,
                                "published": pub,
                                "collected_at": datetime.now(
                                    timezone.utc
                                ).isoformat(),
                                "twitter_handle": handle,
                            })
                            count += 1

                        logger.info(f"    ✅ @{handle}: {count} tweets")
                        fetched = True
                        break

                    except requests.exceptions.Timeout:
                        logger.warning(f"    Timeout (try {attempt + 1})")
                        if attempt < max_retries - 1:
                            time.sleep(retry_delay)

                    except requests.exceptions.ConnectionError:
                        logger.warning(f"    Connection error: {instance_url}")
                        break  # Bu instance down, sonrakine geç

                    except Exception as e:
                        logger.warning(f"    Error: {e}")
                        if attempt < max_retries - 1:
                            time.sleep(retry_delay)

                if not fetched:
                    time.sleep(2)

            if not fetched:
                logger.warning(f"    ❌ @{handle}: ALL instances failed")

            time.sleep(3)

        logger.info(f"  Nitter total: {len(articles)}")
        return articles

        # ══════════════════════════════════════════
    # KEYWORD FILTER
    # ══════════════════════════════════════════

    def keyword_filter(self, articles: List[Dict]) -> List[Dict]:
        """
        2 katmanlı filtre:
          Katman 1 — Konu kelimesi var mı? (airport, hotel, airline, crew...)
          Katman 2 — Olay kelimesi var mı? (attack, bomb, shoot, assault...)
          İkisi de varsa → geçir
        """

        # ── KONU KELİMELERİ (en az 1 tanesi olmalı) ──
        TOPIC_WORDS = {
            "airport", "airfield", "aerodrome", "terminal",
            "airline", "airways", "aviation",
            "flight attendant", "cabin crew", "air hostess",
            "stewardess", "pilot", "copilot", "co-pilot",
            "aircrew", "air crew", "ground crew", "ground staff",
            "hotel", "motel", "resort", "hostel", "inn",
            "lodging", "accommodation",
            "havalimanı", "havaalani", "havayolu",
            "kabin ekibi", "hostes", "pilot",
            "otel", "tatil köyü",
            "aéroport", "hôtel",
            "flughafen",
            "aeropuerto",
        }

        # ── OLAY KELİMELERİ (en az 1 tanesi olmalı) ──
        EVENT_WORDS = {
            "attack", "attacked", "attacker",
            "bomb", "bombing", "bombed", "bomber",
            "shoot", "shooting", "shot", "gunfire", "gunman",
            "explo", "explosion", "explode", "exploded",
            "stab", "stabbing", "stabbed", "knife",
            "assault", "assaulted",
            "terror", "terrorist", "terrorism",
            "siege", "hostage",
            "kill", "killed", "dead", "death", "died",
            "injur", "injured", "wound", "wounded",
            "threat", "threaten",
            "armed", "weapon", "gun", "firearm", "rifle",
            "punch", "punched", "hit", "slap", "slapped",
            "violent", "violence",
            "arson", "fire set", "set fire",
            "raid", "raided", "storm", "stormed",
            "detonate", "detonated", "detonation",
            "suicide", "martyr",
            "saldırı", "saldırıldı", "saldırgan",
            "bomba", "bombalı", "bombalandı",
            "patlama", "patladı",
            "bıçak", "bıçaklandı", "bıçaklı",
            "silahlı", "silah", "ateş açıldı",
            "terör", "terörist",
            "rehine", "rehin",
            "öldürüldü", "öldü", "ölü",
            "yaralı", "yaralandı",
            "darp", "darp edildi", "saldırdı",
            "baskın",
        }

        filtered = []

        for article in articles:
            text = f"{article.get('title', '')} {article.get('summary', '')}".lower()

            # Negatif keyword kontrolü
            has_negative = any(neg in text for neg in self.negative_keywords)
            if has_negative:
                # Negatif varsa çok güçlü eşleşme gerekli
                min_topic = 1
                min_event = 2
            else:
                min_topic = 1
                min_event = 1

            # Konu kelimesi ara
            topic_matches = []
            for tw in TOPIC_WORDS:
                if tw in text:
                    topic_matches.append(tw)

            # Olay kelimesi ara
            event_matches = []
            for ew in EVENT_WORDS:
                if ew in text:
                    event_matches.append(ew)

            # Karar
            if len(topic_matches) >= min_topic and len(event_matches) >= min_event:
                article["keyword_score"] = len(topic_matches) + len(event_matches)
                article["topic_matches"] = topic_matches[:5]
                article["event_matches"] = event_matches[:5]
                article["has_negative_keyword"] = has_negative
                filtered.append(article)

        # Skora göre sırala (en yüksek önce)
        filtered.sort(key=lambda x: x.get("keyword_score", 0), reverse=True)

        return filtered

    # ══════════════════════════════════════
    # HELPERS
    # ══════════════════════════════════════

    def _clean_html(self, text: str) -> str:
        if not text:
            return ""
        try:
            soup = BeautifulSoup(text, "lxml")
            return soup.get_text(separator=" ", strip=True)[:2000]
        except Exception:
            return text[:2000]

    def _clean_nitter_text(self, text: str) -> str:
        if not text:
            return ""
        try:
            soup = BeautifulSoup(text, "lxml")
            clean = soup.get_text(separator=" ", strip=True)
        except Exception:
            clean = text
        clean = re.sub(r"\s+", " ", clean).strip()
        if clean.startswith("R to @") and len(clean) < 30:
            return ""
        return clean[:2000]

    def _nitter_to_twitter_url(self, nitter_url: str, instance: str) -> str:
        if not nitter_url:
            return ""
        try:
            domain = instance.replace("https://", "").replace("http://", "")
            return nitter_url.replace(domain, "x.com").replace("http://", "https://")
        except Exception:
            return nitter_url

    def _parse_date(self, entry) -> str:
        for field in ["published", "updated", "created"]:
            val = entry.get(field)
            if val:
                try:
                    dt = date_parser.parse(val)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt.isoformat()
                except Exception:
                    continue

        if hasattr(entry, "published_parsed") and entry.published_parsed:
            try:
                dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
                return dt.isoformat()
            except Exception:
                pass

        return datetime.now(timezone.utc).isoformat()

    def _within_hours(self, date_str: str, hours: int = 72) -> bool:
        try:
            dt = date_parser.parse(date_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            return (now - dt) <= timedelta(hours=hours)
        except Exception:
            return True
