# scripts/collectors.py

import os
import re
import time
import json
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
                        break

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
    # ARTICLE CONTENT ENRICHMENT
    # ══════════════════════════════════════════

    def enrich_articles(self, articles: List[Dict]) -> List[Dict]:
        """
        Keyword filtreden geçen makalelerin gerçek içeriğini çek.
        RSS'ten sadece başlık + 1-2 cümle geliyor.
        Bu metot asıl haberin metnini alır + gerçek yayın tarihini çıkarır.
        """
        logger.info(f"  Enriching {len(articles)} articles with full content...")

        for i, article in enumerate(articles):
            url = article.get("url", "")
            if not url:
                continue

            # Reddit, Twitter gibi kaynaklarda zaten içerik var
            if article.get("source_type") in ("reddit", "twitter"):
                continue

            # Google News linklerini çözmeye çalışmıyoruz (uberproxy çalışmıyor).
            # RSS'ten gelen title + summary ile yetiniyoruz.
            # Eski olay tespiti LLM prompt'larında agresif stale detection ile yapılıyor.
            if "news.google.com" in url:
                continue

            # Zaten yeterli içerik varsa sadece tarih çıkarımı yap
            needs_content = len(article.get("summary", "")) <= 500

            try:
                resp = self.session.get(url, timeout=10, allow_redirects=True)

                if resp.status_code != 200:
                    continue
                if resp.status_code in (403, 429, 503):
                    # Bot koruması / rate limit — hızlı atla
                    logger.warning(f"    [{i+1}] Blocked ({resp.status_code}): {url[:60]}")
                    continue

                content_type = resp.headers.get("content-type", "")
                if "text/html" not in content_type:
                    continue

                # Ham HTML üzerinden önce tarih çıkar (script tag'leri lazım)
                raw_soup = BeautifulSoup(resp.text, "lxml")
                real_date = self._extract_publish_date(raw_soup)
                if real_date:
                    article["real_publish_date"] = real_date
                    logger.info(f"    [{i+1}] Real date: {real_date}")

                if needs_content:
                    # Gereksiz elementleri kaldır (içerik çıkarımı için)
                    for tag in raw_soup(["script", "style", "nav", "header",
                                     "footer", "aside", "form", "iframe",
                                     "figure", "figcaption", "button"]):
                        tag.decompose()

                    # Makale gövdesini bul — önce yaygın container'ları dene
                    text = ""
                    for container in ["article", "main", ('div', {'role': 'main'}),
                                       ('div', {'class': 'content'}), ('div', {'id': 'content'})]:
                        if isinstance(container, tuple):
                            tag = raw_soup.find(container[0], attrs=container[1])
                        else:
                            tag = raw_soup.find(container)
                        if tag:
                            text = tag.get_text(separator=" ", strip=True)
                            if len(text) > 200:
                                break

                    # Container bulunamadıysa paragraph aggregation
                    if not text:
                        paragraphs = raw_soup.find_all("p")
                        text = " ".join(
                            p.get_text(strip=True) for p in paragraphs
                            if len(p.get_text(strip=True)) > 40
                        )

                    text = re.sub(r"\s+", " ", text).strip()

                    # Yaygın gereksiz metinleri temizle
                    noise_patterns = [
                        r"Cookie Policy.*?\.",
                        r"Subscribe Now.*?\.",
                        r"Sign Up.*?\.",
                        r"Privacy Policy.*?\.",
                        r"Terms of Service.*?\.",
                        r"All rights reserved.*?\.",
                        r"© \d{4}.*?\.",
                        r"Read more.*?\.",
                        r"Related articles.*?\.",
                        r"Follow us on.*?\.",
                        r"Share this article.*?\.",
                        r"Advertisement.*?\.",
                        r"Sponsored content.*?\.",
                        r"Click here to.*?\.",
                        r"Learn more.*?\.",
                    ]
                    for pattern in noise_patterns:
                        text = re.sub(pattern, "", text, flags=re.IGNORECASE)

                    text = re.sub(r"\s+", " ", text).strip()

                    if len(text) > 100:
                        article["summary"] = text[:2000]
                        article["enriched"] = True
                        logger.info(f"    [{i+1}] Enriched: {len(text)} chars → 2000")

            except requests.exceptions.Timeout:
                logger.warning(f"    [{i+1}] Timeout: {url[:60]}")
            except requests.exceptions.HTTPError as e:
                logger.warning(f"    [{i+1}] HTTP error: {e}")
            except Exception as e:
                logger.warning(f"    [{i+1}] Enrich error: {str(e)[:80]}")

            time.sleep(0.5)

        enriched_count = sum(1 for a in articles if a.get("enriched"))
        date_count = sum(1 for a in articles if a.get("real_publish_date"))
        logger.info(f"  Enriched {enriched_count}/{len(articles)} articles, {date_count} real dates found")

        return articles

    def _extract_publish_date(self, soup: BeautifulSoup) -> str:
        """
        HTML sayfasından gerçek yayınlanma tarihini çıkar.
        Öncelik sırası:
        1. <meta property="article:published_time">
        2. <meta name="date" / name="publish_date" / name="pubdate">
        3. <meta property="og:article:published_time">
        4. <time datetime="...">
        5. JSON-LD (application/ld+json) içindeki datePublished
        """
        # 1. article:published_time meta tag
        for attr in ["article:published_time", "og:article:published_time"]:
            tag = soup.find("meta", attrs={"property": attr})
            if tag and tag.get("content"):
                parsed = self._try_parse_date(tag["content"])
                if parsed:
                    return parsed

        # 2. name-based meta tags
        for name in ["date", "publish_date", "pubdate", "publishdate",
                     "article_date", "article:date", "DC.date.issued"]:
            tag = soup.find("meta", attrs={"name": re.compile(name, re.I)})
            if tag and tag.get("content"):
                parsed = self._try_parse_date(tag["content"])
                if parsed:
                    return parsed

        # 3. <time> element with datetime attribute
        time_tag = soup.find("time", attrs={"datetime": True})
        if time_tag:
            parsed = self._try_parse_date(time_tag["datetime"])
            if parsed:
                return parsed

        # 4. JSON-LD structured data
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                ld_data = json.loads(script.string or "")
                # Tek obje veya liste olabilir
                items = ld_data if isinstance(ld_data, list) else [ld_data]
                for item in items:
                    dp = item.get("datePublished")
                    if dp:
                        parsed = self._try_parse_date(dp)
                        if parsed:
                            return parsed
                    # @graph yapısı
                    for graph_item in item.get("@graph", []):
                        dp = graph_item.get("datePublished")
                        if dp:
                            parsed = self._try_parse_date(dp)
                            if parsed:
                                return parsed
            except (json.JSONDecodeError, AttributeError, TypeError):
                continue

        return ""

    def _try_parse_date(self, date_str: str) -> str:
        """Tarih string'ini YYYY-MM-DD formatına çevir. Başarısızsa boş string döndür."""
        if not date_str or len(date_str) < 8:
            return ""
        try:
            dt = date_parser.parse(date_str)
            # Makul bir tarih mi? (2024-2030 arası — eski olayları filtrele)
            if dt.year < 2024 or dt.year > 2030:
                return ""
            return dt.strftime("%Y-%m-%d")
        except Exception:
            return ""

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

        # Ek negatif keyword'ler (config'dekilerin üzerine)
        EXTRA_NEGATIVE = {
            "drill", "exercise", "simulation", "training",
            "anniversary", "remembering", "memorial", "commemoration",
            "years since", "look back", "retrospective",
            "movie", "film", "book review", "novel",
            "video game", "game release",
            "stock market", "shares", "investment",
            "strike action", "labor dispute", "walkout", "pay dispute",
            "delayed flight", "cancelled flight", "weather delay",
            "hotel review", "hotel rating", "star hotel",
            "tourism record", "travel tips", "booking", "reservation",
            "renovation", "grand opening",
            "new security system", "security upgrade", "security technology",
        }
        all_negative = set(self.negative_keywords) | EXTRA_NEGATIVE

        def _word_in_text(word: str, text: str) -> bool:
            """Tam kelime sınırı kontrolü yap — 'airport' 'airportable' içinde yakalanmasın."""
            # Çok kelimeli ifadeler için regex kullan
            if len(word.split()) > 1:
                return word in text
            # Tek kelimeler için \b sınır kontrolü
            return bool(re.search(rf'\b{re.escape(word)}\b', text))

        filtered = []

        for article in articles:
            text = f"{article.get('title', '')} {article.get('summary', '')}".lower()

            has_negative = any(_word_in_text(neg, text) for neg in all_negative)
            if has_negative:
                min_topic = 1
                min_event = 2
            else:
                min_topic = 1
                min_event = 1

            topic_matches = []
            for tw in TOPIC_WORDS:
                if _word_in_text(tw, text):
                    topic_matches.append(tw)

            event_matches = []
            for ew in EVENT_WORDS:
                if _word_in_text(ew, text):
                    event_matches.append(ew)

            if len(topic_matches) >= min_topic and len(event_matches) >= min_event:
                article["keyword_score"] = len(topic_matches) + len(event_matches)
                article["topic_matches"] = topic_matches[:5]
                article["event_matches"] = event_matches[:5]
                article["has_negative_keyword"] = has_negative
                filtered.append(article)

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
        clean = re.sub(r"pic\.twitter\.com/\S+", "", clean).strip()
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
            return (datetime.now(timezone.utc) - dt) <= timedelta(hours=hours)
        except Exception:
            return True
