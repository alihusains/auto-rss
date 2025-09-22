#!/usr/bin/env python3
"""
rss_to_telegram.py
Production-ready RSS -> Telegram poster.

Config via environment variables or CLI:
 - TELEGRAM_BOT_TOKEN (env)  : required
 - TELEGRAM_CHAT_ID   (env)  : required (channel @username or -100... id)
 - FEEDS_CSV_URL      (env or --feeds-csv) : required (published Google Sheet CSV URL)
 - POSTED_FILE        (env or --posted-file) default: posted.json
 - FUZZY_THRESHOLD    (env or --fuzzy-threshold) default: 88

Behavior:
 - Reads CSV of feeds (columns: feed_url/url/rss/feed and optional category)
 - Parses feeds with feedparser
 - Deduplicates (exact link + fuzzy title)
 - Attempts to fetch article text & top image with newspaper3k
 - Summarizes with Sumy TextRank (fallback to first sentences)
 - Posts to Telegram channel (sendPhoto if image, else sendMessage)
 - Persists posted fingerprints to posted.json
"""

from __future__ import annotations
import os
import sys
import csv
import json
import time
import hashlib
import logging
import argparse
from datetime import datetime, timezone
from typing import List, Dict, Optional

import requests
import feedparser
from rapidfuzz import fuzz
from newspaper import Article
# correct Sumy import:
from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.text_rank import TextRankSummarizer

# ---------- config defaults ----------
DEFAULT_POSTED_FILE = "posted.json"
DEFAULT_FUZZY_THRESHOLD = 88
TELEGRAM_API_BASE = "https://api.telegram.org/bot{token}/{method}"
TELEGRAM_SENDMSG = "sendMessage"
TELEGRAM_SENDPHOTO = "sendPhoto"
SLEEP_BETWEEN_POSTS = 0.8  # polite delay

# ---------- logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------- utilities ----------
def load_posted(path: str) -> Dict:
    if not os.path.exists(path):
        return {"items": []}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logging.warning("Failed to load posted file %s: %s", path, e)
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
        logging.debug("Sumy summarize failed: %s", e)
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
    r = requests.post(url, data=payload, timeout=30)
    logging.info("sendMessage status=%s", r.status_code)
    return r

def telegram_send_photo(token: str, chat_id: str, photo_url: str, caption: Optional[str] = None, parse_mode: str = "HTML"):
    url = TELEGRAM_API_BASE.format(token=token, method=TELEGRAM_SENDPHOTO)
    payload = {
        "chat_id": chat_id,
        "photo": photo_url,
        "parse_mode": parse_mode
    }
    if caption:
        payload["caption"] = caption[:900]  # keep safe
    r = requests.post(url, data=payload, timeout=30)
    logging.info("sendPhoto status=%s", r.status_code)
    return r

# ---------- article fetch ----------
def fetch_article(url: str) -> Dict[str, Optional[str]]:
    art = Article(url)
    try:
        art.download()
        art.parse()
        # art.nlp() may require punkt; but we are using sumy for summarization
    except Exception as e:
        logging.debug("newspaper fetch error for %s: %s", url, e)
        return {"text": None, "top_image": None, "title": None}
    return {"text": art.text or None, "top_image": art.top_image or None, "title": art.title or None}

# ---------- feed list reader ----------
def read_feeds_csv(source: str) -> List[Dict[str, str]]:
    """
    Accepts URL (http(s)) or local path.
    Supports common header names: feed_url, url, rss, feed
    Also supports category headers: category, cat, tag
    """
    feeds = []
    if source.startswith("http://") or source.startswith("https://"):
        r = requests.get(source, timeout=30)
        r.raise_for_status()
        text = r.text
        reader = csv.DictReader(text.splitlines())
    else:
        reader = csv.DictReader(open(source, "r", encoding="utf-8"))
    for row in reader:
        url = (row.get("feed_url") or row.get("url") or row.get("rss") or row.get("feed") or "").strip()
        category = (row.get("category") or row.get("cat") or row.get("tag") or "").strip()
        if url:
            feeds.append({"url": url, "category": category})
    return feeds

# ---------- dedupe ----------
def is_duplicate(posted_data: Dict, entry_link: str, entry_title: str, fuzzy_threshold: int = DEFAULT_FUZZY_THRESHOLD):
    # exact link
    for item in posted_data.get("items", []):
        if item.get("link") and item["link"] == entry_link and entry_link:
            return True, "exact_link"
    # fuzzy title
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

# ---------- main loop ----------
def process(feeds_csv: str, telegram_token: str, telegram_chat: str, posted_file: str, fuzzy_threshold: int):
    feeds = read_feeds_csv(feeds_csv)
    logging.info("Loaded %d feeds", len(feeds))
    posted = load_posted(posted_file)
    new_count = 0

    for f in feeds:
        url = f["url"]
        logging.info("Checking feed: %s (category=%s)", url, f.get("category", ""))
        try:
            d = feedparser.parse(url)
        except Exception as e:
            logging.warning("feedparser failed for %s: %s", url, e)
            continue
        entries = d.get("entries", [])
        for e in entries:
            link = e.get("link") or e.get("id") or ""
            title = e.get("title") or ""
            published = e.get("published") or e.get("updated") or ""
            # skip if no useful id/title
            if not link and not title:
                continue
            dup, reason = is_duplicate(posted, link, title, fuzzy_threshold)
            if dup:
                logging.debug("Skipping duplicate: %s (%s)", title[:120], reason)
                continue

            # build content: try article extraction
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
                        # fallback to feed content
                        content = ""
                        if "content" in e and e["content"]:
                            content = e["content"][0].get("value", "")
                        elif "summary" in e:
                            content = e["summary"]
                        summary_text = first_n_sentences(content, n=3)
                        # try media
                        if e.get("media_content"):
                            top_image = e.get("media_content", [{}])[0].get("url")
                        elif e.get("enclosures"):
                            top_image = e.get("enclosures", [{}])[0].get("url")
            except Exception as ex:
                logging.warning("Article fetch/summarize failed: %s", ex)
                summary_text = (e.get("summary") or "")[:400]

            # prepare message (HTML)
            title_html = f"<b>{escape_html(title)}</b>"
            cat_html = f" <i>{escape_html(f.get('category',''))}</i>" if f.get("category") else ""
            source_html = f"\n\nSource: <a href=\"{escape_html(link)}\">Read original</a>" if link else ""
            body_html = escape_html(summary_text)[:900]

            caption = title_html + cat_html + "\n" + body_html + source_html

            # send
            posted_ok = False
            try:
                if top_image:
                    r = telegram_send_photo(telegram_token, telegram_chat, top_image, caption=caption)
                    if r.status_code != 200:
                        logging.warning("sendPhoto failed (status=%s); fallback to sendMessage", r.status_code)
                        telegram_send_message(telegram_token, telegram_chat, title_html + "\n" + body_html + source_html)
                else:
                    telegram_send_message(telegram_token, telegram_chat, title_html + "\n" + body_html + source_html)
                # record
                record_posted(posted, link, title)
                new_count += 1
                posted_ok = True
            except Exception as post_ex:
                logging.exception("Failed to post to Telegram: %s", post_ex)

            time.sleep(SLEEP_BETWEEN_POSTS)

    if new_count:
        save_posted(posted_file, posted)
    logging.info("Processing complete. New posts: %d", new_count)


# ---------- CLI ----------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--feeds-csv", default=os.environ.get("FEEDS_CSV_URL", ""), help="Feeds CSV URL or local path")
    parser.add_argument("--posted-file", default=os.environ.get("POSTED_JSON", DEFAULT_POSTED_FILE))
    parser.add_argument("--fuzzy-threshold", type=int, default=int(os.environ.get("FUZZY_THRESHOLD", DEFAULT_FUZZY_THRESHOLD)))
    args = parser.parse_args()

    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
    FEEDS_CSV = args.feeds_csv

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID or not FEEDS_CSV:
        logging.error("Missing required env vars. Set TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID and FEEDS_CSV_URL or pass --feeds-csv")
        sys.exit(2)

    # ensure NLTK punkt is available (harmless if already present)
    try:
        import nltk
        nltk.data.find("tokenizers/punkt")
    except Exception:
        try:
            import nltk
            nltk.download("punkt")
        except Exception:
            logging.warning("Failed to download NLTK punkt; summarization may be degraded.")

    process(FEEDS_CSV, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, args.posted_file, args.fuzzy_threshold)


if __name__ == "__main__":
    main()
