#!/usr/bin/env python3
"""
rss_to_telegram.py
Production-ready RSS -> Telegram poster.

Usage (env vars or CLI):
  TELEGRAM_BOT_TOKEN (env)  - required
  TELEGRAM_CHAT_ID   (env)  - required (channel @username or -100... id)
  FEEDS_CSV_URL      (env or --feeds-csv) - required
  POSTED_FILE        (env or --posted-file) default: posted.json
  FUZZY_THRESHOLD    (env or --fuzzy-threshold) default: 88

Behavior:
 - Reads CSV of feeds (feed_url/url/rss/feed and optional category)
 - Parses feeds (feedparser)
 - Deduplicates (exact link + fuzzy title)
 - Attempts to fetch article text & top image (newspaper3k)
 - Summarizes with Sumy TextRank (fallback to first sentences)
 - Posts to Telegram (sendPhoto if image else sendMessage)
 - Persists posted fingerprints to posted.json
"""

from __future__ import annotations
import argparse
import csv
import json
import os
import sys
import time
import logging
import hashlib
from datetime import datetime, timezone
from typing import Dict, List, Optional

import requests
import feedparser
from rapidfuzz import fuzz
from newspaper import Article

# Sumy import (use text_rank)
from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.text_rank import TextRankSummarizer

# ---------- Config ----------
DEFAULT_POSTED_FILE = "posted.json"
DEFAULT_FUZZY_THRESHOLD = 88
TELEGRAM_API_BASE = "https://api.telegram.org/bot{token}/{method}"
TELEGRAM_SENDMSG = "sendMessage"
TELEGRAM_SENDPHOTO = "sendPhoto"
SLEEP_BETWEEN_POSTS = 0.8  # polite gap

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------- Utilities ----------
def int_from_env(name: str, fallback: int) -> int:
    v = os.environ.get(name)
    if v is None:
        return fallback
    v = v.strip()
    if v == "":
        return fallback
    try:
        return int(v)
    except Exception:
        return fallback

def load_posted(path: str) -> Dict:
    if not os.path.exists(path):
        return {"items": []}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning("Could not read posted file %s: %s", path, e)
        return {"items": []}

def save_posted(path: str, data: Dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def sha1_text(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def normalize_text(s: Optional[str]) -> str:
    if not s:
        return ""
    return " ".join(s.strip().lower().split())

def first_n_sentences(text: str, n: int = 2) -> str:
    if not text:
        return ""
    # naive sentence split
    parts = [p.strip() for p in text.replace("\r", " ").split(".") if p.strip()]
    return (". ".join(parts[:n]) + (". " if len(parts[:n])>0 else "")).strip()

def summarize_text(text: str, sentences: int = 3) -> str:
    if not text or len(text.strip()) < 30:
        return first_n_sentences(text, n=2)
    try:
        parser = PlaintextParser.from_string(text, Tokenizer("english"))
        summarizer = TextRankSummarizer()
        sents = summarizer(parser.document, sentences)
        out = " ".join(str(s) for s in sents)
        if not out.strip():
            return first_n_sentences(text, n=2)
        return out
    except Exception as e:
        logger.debug("Sumy error: %s", e)
        return first_n_sentences(text, n=2)

def escape_html(s: Optional[str]) -> str:
    if not s:
        return ""
    return (s.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;"))

# ---------- Telegram helpers ----------
def telegram_send_message(token: str, chat_id: str, text: str, parse_mode: str = "HTML", disable_preview: bool = False):
    url = TELEGRAM_API_BASE.format(token=token, method=TELEGRAM_SENDMSG)
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": disable_preview
    }
    try:
        r = requests.post(url, data=payload, timeout=30)
        logger.info("sendMessage status=%s", r.status_code)
        return r
    except Exception as e:
        logger.exception("sendMessage exception: %s", e)
        raise

def telegram_send_photo(token: str, chat_id: str, photo_url: str, caption: Optional[str] = None, parse_mode: str = "HTML"):
    url = TELEGRAM_API_BASE.format(token=token, method=TELEGRAM_SENDPHOTO)
    payload = {
        "chat_id": chat_id,
        "photo": photo_url,
        "parse_mode": parse_mode
    }
    if caption:
        payload["caption"] = caption[:900]
    try:
        r = requests.post(url, data=payload, timeout=30)
        logger.info("sendPhoto status=%s", r.status_code)
        return r
    except Exception as e:
        logger.exception("sendPhoto exception: %s", e)
        raise

# ---------- Article fetch ----------
def fetch_article(url: str) -> Dict[str, Optional[str]]:
    art = Article(url)
    try:
        art.download()
        art.parse()
    except Exception as e:
        logger.debug("newspaper error for %s: %s", url, e)
        return {"text": None, "top_image": None, "title": None}
    return {"text": art.text or None, "top_image": art.top_image or None, "title": art.title or None}

# ---------- Feeds reader ----------
def read_feeds_csv(source: str) -> List[Dict[str,str]]:
    feeds: List[Dict[str,str]] = []
    try:
        if source.startswith("http://") or source.startswith("https://"):
            r = requests.get(source, timeout=30)
            r.raise_for_status()
            text = r.text
            reader = csv.DictReader(text.splitlines())
        else:
            reader = csv.DictReader(open(source, "r", encoding="utf-8"))
    except Exception as e:
        logger.error("Failed to read feeds CSV %s: %s", source, e)
        return feeds

    for row in reader:
        url = (row.get("feed_url") or row.get("url") or row.get("rss") or row.get("feed") or row.get("Feed") or row.get("URL") or "").strip()
        category = (row.get("category") or row.get("cat") or row.get("tag") or row.get("Category") or "").strip()
        if url:
            feeds.append({"url": url, "category": category})
    return feeds

# ---------- dedupe ----------
def is_duplicate(posted_data: Dict, entry_link: str, entry_title: str, fuzzy_threshold: int):
    if entry_link:
        for item in posted_data.get("items", []):
            if item.get("link") and item["link"] == entry_link:
                return True, "exact_link"
    title_norm = normalize_text(entry_title)
    if title_norm:
        for item in posted_data.get("items", []):
            t = normalize_text(item.get("title", ""))
            if not t:
                continue
            score = fuzz.token_set_ratio(title_norm, t)
            if score >= fuzzy_threshold:
                return True, f"fuzzy_title({score})"
    return False, None

def record_posted(posted_data: Dict, entry_link: str, title: str):
    fingerprint = sha1_text((entry_link or "") + "||" + (title or ""))
    posted_data.setdefault("items", []).append({
        "link": entry_link,
        "title": title,
        "fingerprint": fingerprint,
        "posted_at": datetime.now(timezone.utc).isoformat()
    })

# ---------- Main processing ----------
def process(feeds_csv: str, telegram_token: str, telegram_chat: str, posted_file: str, fuzzy_threshold: int):
    feeds = read_feeds_csv(feeds_csv)
    logger.info("Loaded %d feeds", len(feeds))
    posted = load_posted(posted_file)
    new_count = 0

    for f in feeds:
        url = f["url"]
        logger.info("Checking feed: %s (category=%s)", url, f.get("category", ""))
        try:
            d = feedparser.parse(url)
        except Exception as e:
            logger.warning("feedparser failed for %s: %s", url, e)
            continue

        entries = d.get("entries", [])
        for e in entries:
            link = e.get("link") or e.get("id") or ""
            title = e.get("title") or ""
            if not link and not title:
                continue

            dup, reason = is_duplicate(posted, link, title, fuzzy_threshold)
            if dup:
                logger.debug("Skipping duplicate: %s (%s)", title[:120], reason)
                continue

            # fetch article text & image
            top_image = None
            summary_text = ""
            try:
                if link:
                    article = fetch_article(link)
                    if article and article.get("text"):
                        summary_text = summarize_text(article["text"], sentences=3)
                        if not summary_text or len(summary_text) < 60:
                            summary_text = first_n_sentences(article["text"], n=2)
                        top_image = article.get("top_image")
                    else:
                        # fallback to feed summary/content
                        content = ""
                        if "content" in e and e["content"]:
                            content = e["content"][0].get("value", "")
                        elif "summary" in e:
                            content = e["summary"]
                        summary_text = first_n_sentences(content, n=3)
                        if e.get("media_content"):
                            top_image = e.get("media_content", [{}])[0].get("url")
                        elif e.get("enclosures"):
                            top_image = e.get("enclosures", [{}])[0].get("url")
            except Exception as ex:
                logger.warning("Article fetch/summarize failed for %s: %s", link, ex)
                summary_text = (e.get("summary") or "")[:400]

            # Prepare HTML message
            title_html = f"<b>{escape_html(title)}</b>"
            cat_html = f" <i>{escape_html(f.get('category',''))}</i>" if f.get("category") else ""
            source_html = f"\n\nSource: <a href=\"{escape_html(link)}\">Read original</a>" if link else ""
            body_html = escape_html(summary_text)[:900]
            caption = title_html + cat_html + "\n" + body_html + source_html

            # Post to Telegram
            try:
                if top_image:
                    r = telegram_send_photo(telegram_token, telegram_chat, top_image, caption=caption)
                    if r.status_code != 200:
                        logger.warning("sendPhoto failed (status=%s); fallback to sendMessage", r.status_code)
                        telegram_send_message(telegram_token, telegram_chat, title_html + "\n" + body_html + source_html)
                else:
                    telegram_send_message(telegram_token, telegram_chat, title_html + "\n" + body_html + source_html)

                record_posted(posted, link, title)
                new_count += 1
            except Exception as post_ex:
                logger.exception("Failed to post to Telegram: %s", post_ex)

            time.sleep(SLEEP_BETWEEN_POSTS)

    if new_count:
        save_posted(posted_file, posted)
    logger.info("Processing complete. New posts: %d", new_count)

# ---------- CLI ----------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--feeds-csv", default=os.environ.get("FEEDS_CSV_URL", ""), help="Feeds CSV URL or local path")
    parser.add_argument("--posted-file", default=os.environ.get("POSTED_JSON", DEFAULT_POSTED_FILE))
    # safe env parsing for fuzzy threshold
    parser.add_argument("--fuzzy-threshold", type=int, default=int_from_env("FUZZY_THRESHOLD", DEFAULT_FUZZY_THRESHOLD))
    args = parser.parse_args()

    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
    FEEDS_CSV = args.feeds_csv

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID or not FEEDS_CSV:
        logger.error("Missing required config. Set TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID and FEEDS_CSV_URL (or pass --feeds-csv).")
        sys.exit(2)

    # ensure NLTK punkt available (harmless if already installed)
    try:
        import nltk
        try:
            nltk.data.find("tokenizers/punkt")
        except Exception:
            nltk.download("punkt")
    except Exception:
        logger.debug("nltk not available or failed to download punkt; summarization may be degraded.")

    process(FEEDS_CSV, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, args.posted_file, args.fuzzy_threshold)

if __name__ == "__main__":
    main()
