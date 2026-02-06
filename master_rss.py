#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master RSS Generator - generuje kanały RSS/JSON z wielu źródeł.

Źródła:
  - bankier -> https://www.bankier.pl (news + gielda połączone)
  - pap     -> https://biznes.pap.pl/kategoria/depesze-pap

Użycie:
    python master_rss.py --all              # generuje wszystkie feedy
    python master_rss.py --source bankier   # tylko bankier
    python master_rss.py --source pap       # tylko PAP
"""

import logging
import time
import json
import re
import argparse
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from urllib.parse import urljoin

import pytz
import requests
from bs4 import BeautifulSoup
from feedgen.feed import FeedGenerator

# --------------------------------------------------------------------
# KONFIGURACJA
# --------------------------------------------------------------------

SLEEP_BETWEEN_REQUESTS = 2.5
HOURS_BACK = 24
TZ_WARSAW = pytz.timezone("Europe/Warsaw")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "close",
}

# Konfiguracja źródeł
SOURCES = {
    "bankier": {
        "name": "Bankier.pl – Wiadomości",
        "base_url": "https://www.bankier.pl",
        "urls": [
            ("https://www.bankier.pl/wiadomosc/", 5, "bankier_news"),
            ("https://www.bankier.pl/gielda/wiadomosci/", 5, "bankier_gielda"),
        ],
        "rss_file": "bankier-rss.xml",
        "json_file": "bankier-feed.json",
    },
    "pap": {
        "name": "PAP Biznes – Depesze",
        "base_url": "https://biznes.pap.pl",
        "urls": [
            ("https://biznes.pap.pl/kategoria/depesze-pap", 10, "pap"),
        ],
        "rss_file": "pap-rss.xml",
        "json_file": "pap-feed.json",
    },
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# --------------------------------------------------------------------
# FUNKCJE POMOCNICZE
# --------------------------------------------------------------------

def fetch_page_html(url: str) -> Optional[str]:
    logging.info("Pobieram: %s", url)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        return resp.text
    except Exception as exc:
        logging.error("Błąd pobierania %s: %s", url, exc)
        return None
    finally:
        time.sleep(SLEEP_BETWEEN_REQUESTS)

# --------------------------------------------------------------------
# PARSERY
# --------------------------------------------------------------------

def parse_bankier_news(html: str, base_url: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    section = soup.find("section", id="articleList")
    if not section:
        return []
    
    articles = []
    for art in section.find_all("div", class_="article", recursive=False):
        try:
            content = art.find("div", class_="entry-content")
            if not content:
                continue
            title_span = content.find("span", class_="entry-title")
            if not title_span:
                continue
            a_tag = title_span.find("a")
            if not a_tag or not a_tag.get("href"):
                continue

            title = " ".join(a_tag.get_text(strip=True).split())
            link = urljoin(base_url, a_tag["href"])

            meta_div = content.find("div", class_="entry-meta")
            if not meta_div:
                continue
            time_tags = meta_div.find_all("time", class_="entry-date")
            if not time_tags:
                continue

            dt_str = time_tags[-1].get("datetime") or time_tags[-1].get_text(strip=True)
            pub_dt = datetime.fromisoformat(dt_str)
            if pub_dt.tzinfo is None:
                pub_dt = TZ_WARSAW.localize(pub_dt)
            else:
                pub_dt = pub_dt.astimezone(TZ_WARSAW)

            teaser_tag = content.find("p")
            teaser = ""
            if teaser_tag:
                for more_link in teaser_tag.find_all("a", class_="more-link"):
                    more_link.decompose()
                teaser = " ".join(teaser_tag.get_text(" ", strip=True).split())

            articles.append({"title": title, "link": link, "pub_date": pub_dt, "teaser": teaser})
        except Exception:
            continue
    return articles


def parse_bankier_gielda(html: str, base_url: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    main = soup.find("main") or soup
    articles = []
    pattern = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2})\s+(.+)$")

    for a in main.find_all("a", href=True):
        text = " ".join(a.get_text(" ", strip=True).split())
        m = pattern.match(text)
        if not m:
            continue
        dt_str, title = m.groups()
        try:
            dt_naive = datetime.strptime(dt_str, "%Y-%m-%d %H:%M")
        except ValueError:
            continue
        pub_dt = TZ_WARSAW.localize(dt_naive)
        link = urljoin(base_url, a["href"])
        articles.append({"title": title, "link": link, "pub_date": pub_dt, "teaser": ""})
    return articles


def parse_pap(html: str, base_url: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    articles = []
    now = datetime.now(TZ_WARSAW)

    for a_tag in soup.find_all("a", href=True):
        href = a_tag.get("href", "")
        if "/wiadomosci/" not in href or "/kategoria/" in href:
            continue
        if href.endswith("/wiadomosci/"):
            continue
        
        link = urljoin(base_url, href)
        title = " ".join(a_tag.get_text(strip=True).split())
        if not title or len(title) < 10:
            continue
        
        teaser = ""
        parent = a_tag.find_parent()
        if parent:
            p_tag = parent.find_next_sibling("p") or parent.find("p")
            if p_tag:
                teaser = " ".join(p_tag.get_text(strip=True).split())[:200]
        
        articles.append({"title": title, "link": link, "pub_date": now, "teaser": teaser})
    return articles


PARSERS = {
    "bankier_news": parse_bankier_news,
    "bankier_gielda": parse_bankier_gielda,
    "pap": parse_pap,
}

# --------------------------------------------------------------------
# ZBIERANIE ARTYKUŁÓW
# --------------------------------------------------------------------

def collect_articles(source_key: str) -> List[Dict]:
    """Zbiera artykuły z danego źródła (może mieć wiele URL-i)."""
    config = SOURCES[source_key]
    base_url = config["base_url"]
    
    all_articles = []
    seen_links = set()
    now = datetime.now(TZ_WARSAW)
    cutoff = now - timedelta(hours=HOURS_BACK)

    for section_url, num_pages, parser_name in config["urls"]:
        parser = PARSERS[parser_name]
        
        for page in range(1, num_pages + 1):
            if "pap" in parser_name:
                url = section_url if page == 1 else f"{section_url}?page={page}"
            else:
                url = section_url if page == 1 else f"{section_url}{page}"

            html = fetch_page_html(url)
            if not html:
                continue

            for art in parser(html, base_url):
                if art["link"] in seen_links:
                    continue
                if art["pub_date"] < cutoff:
                    continue
                seen_links.add(art["link"])
                all_articles.append(art)

    all_articles.sort(key=lambda x: x["pub_date"], reverse=True)
    logging.info("[%s] Znaleziono %d artykułów (bez duplikatów)", source_key, len(all_articles))
    return all_articles

# --------------------------------------------------------------------
# GENERATORY
# --------------------------------------------------------------------

def generate_rss(articles: List[Dict], config: dict) -> bytes:
    fg = FeedGenerator()
    fg.load_extension("dc")
    fg.title(config["name"])
    fg.link(href=config["base_url"], rel="alternate")
    fg.description(f"Automatyczny kanał RSS – {config['name']} (ostatnie {HOURS_BACK}h)")
    fg.language("pl")
    if articles:
        fg.lastBuildDate(articles[0]["pub_date"])
    for art in articles:
        fe = fg.add_entry()
        fe.id(art["link"])
        fe.link(href=art["link"])
        fe.title(art["title"])
        if art["teaser"]:
            fe.description(art["teaser"])
        fe.pubDate(art["pub_date"])
    return fg.rss_str(pretty=True)


def generate_json(articles: List[Dict], config: dict) -> str:
    return json.dumps({
        "version": "https://jsonfeed.org/version/1",
        "title": config["name"],
        "home_page_url": config["base_url"],
        "items": [
            {"id": a["link"], "url": a["link"], "title": a["title"],
             "content_html": a["teaser"], "date_published": a["pub_date"].isoformat()}
            for a in articles
        ],
    }, ensure_ascii=False, indent=2)

# --------------------------------------------------------------------
# MAIN
# --------------------------------------------------------------------

def process_source(source_key: str, output_dir: str):
    config = SOURCES[source_key]
    articles = collect_articles(source_key)
    
    rss_path = os.path.join(output_dir, config["rss_file"])
    json_path = os.path.join(output_dir, config["json_file"])
    
    with open(rss_path, "wb") as f:
        f.write(generate_rss(articles, config))
    with open(json_path, "w", encoding="utf-8") as f:
        f.write(generate_json(articles, config))
    
    logging.info("[%s] Zapisano: %s, %s", source_key, rss_path, json_path)


def main():
    parser = argparse.ArgumentParser(description="Master RSS Generator")
    parser.add_argument("--all", action="store_true", help="Generuj wszystkie feedy")
    parser.add_argument("--source", choices=["bankier", "pap"], help="Wybrane źródło")
    parser.add_argument("--output", default="docs", help="Katalog wyjściowy")
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)

    if args.all:
        sources = list(SOURCES.keys())
    elif args.source:
        sources = [args.source]
    else:
        sources = list(SOURCES.keys())

    for src in sources:
        process_source(src, args.output)

    logging.info("Gotowe!")


if __name__ == "__main__":
    main()
