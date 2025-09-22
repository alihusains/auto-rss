# from __future__ import annotations
# import argparse
# import csv
# import json
# import os
# import sys
# import time
# import logging
# import hashlib
# import tempfile
# import urllib.parse
# from datetime import datetime, timezone
# from typing import Dict, Optional, List

# import requests
# import feedparser
# from rapidfuzz import fuzz
# from newspaper import Article

# # Sumy TextRank import
# from sumy.parsers.plaintext import PlaintextParser
# from sumy.nlp.tokenizers import Tokenizer
# from sumy.summarizers.text_rank import TextRankSummarizer

# # ---------- Config ----------
# DEFAULT_POSTED_FILE = "posted.json"
# DEFAULT_FUZZY_THRESHOLD = 88
# TELEGRAM_API_BASE = "https://api.telegram.org/bot{token}/{method}"
# TELEGRAM_SENDMSG = "sendMessage"
# TELEGRAM_SENDPHOTO = "sendPhoto"
# SLEEP_BETWEEN_POSTS = 0.8  # polite delay between posts
# PHOTO_CAPTION_LIMIT = 1024  # Telegram caption limit for photos

# # ---------- Logging ----------
# logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
# logger = logging.getLogger(__name__)

# # ---------- Helpers ----------
# def int_from_env(name: str, fallback: int) -> int:
#     v = os.environ.get(name)
#     if v is None:
#         return fallback
#     v = v.strip()
#     if v == "":
#         return fallback
#     try:
#         return int(v)
#     except Exception:
#         return fallback

# def sha1_text(s: str) -> str:
#     return hashlib.sha1(s.encode("utf-8")).hexdigest()

# def normalize_text(s: Optional[str]) -> str:
#     if not s:
#         return ""
#     return " ".join(s.strip().lower().split())

# def first_n_sentences(text: str, n: int = 2) -> str:
#     if not text:
#         return ""
#     parts = [p.strip() for p in text.replace("\r", " ").split(".") if p.strip()]
#     return (". ".join(parts[:n]) + (". " if len(parts[:n])>0 else "")).strip()

# def summarize_text(text: str, sentences: int = 3) -> str:
#     if not text or len(text.strip()) < 30:
#         return first_n_sentences(text, n=2)
#     try:
#         parser = PlaintextParser.from_string(text, Tokenizer("english"))
#         summarizer = TextRankSummarizer()
#         sents = summarizer(parser.document, sentences)
#         out = " ".join(str(s) for s in sents)
#         return out if out.strip() else first_n_sentences(text, n=2)
#     except Exception as e:
#         logger.debug("Sumy summarize failure: %s", e)
#         return first_n_sentences(text, n=2)

# def escape_html(s: Optional[str]) -> str:
#     if not s:
#         return ""
#     return (s.replace("&", "&amp;")
#              .replace("<", "&lt;")
#              .replace(">", "&gt;"))

# def extract_site_name(url: str) -> str:
#     """Return human-friendly site name (domain without www)."""
#     try:
#         p = urllib.parse.urlparse(url)
#         host = p.netloc or ""
#         if host.startswith("www."):
#             host = host[4:]
#         return host
#     except Exception:
#         return ""

# # ---------- Persistence ----------
# def load_posted(path: str) -> Dict:
#     if not os.path.exists(path):
#         return {"items": []}
#     try:
#         with open(path, "r", encoding="utf-8") as f:
#             return json.load(f)
#     except Exception as e:
#         logger.warning("Failed to load posted file %s: %s", path, e)
#         return {"items": []}

# def save_posted(path: str, data: Dict) -> None:
#     with open(path, "w", encoding="utf-8") as f:
#         json.dump(data, f, ensure_ascii=False, indent=2)

# def record_posted(posted_data: Dict, entry_link: str, title: str, summary: str = "", top_image: Optional[str] = None, published: Optional[str] = None, category: Optional[str] = None):
#     fingerprint = sha1_text((entry_link or "") + "||" + (title or ""))
#     posted_data.setdefault("items", []).append({
#         "link": entry_link,
#         "title": title,
#         "summary": summary,
#         "top_image": top_image,
#         "published": published,
#         "category": category,
#         "fingerprint": fingerprint,
#         "posted_at": datetime.now(timezone.utc).isoformat()
#     })

# # ---------- Telegram helpers ----------
# def _log_response(r: requests.Response) -> None:
#     try:
#         body = r.json()
#     except Exception:
#         body = r.text
#     logger.info("Telegram API response status=%s body=%s", r.status_code, body)

# def telegram_send_message(token: str, chat_id: str, text: str, parse_mode: str = "HTML", disable_preview: bool = False) -> requests.Response:
#     url = TELEGRAM_API_BASE.format(token=token, method=TELEGRAM_SENDMSG)
#     payload = {
#         "chat_id": chat_id,
#         "text": text,
#         "parse_mode": parse_mode,
#         "disable_web_page_preview": disable_preview
#     }
#     r = requests.post(url, data=payload, timeout=30)
#     _log_response(r)
#     return r

# def telegram_send_photo_url(token: str, chat_id: str, photo_url: str, caption: Optional[str] = None, parse_mode: str = "HTML") -> requests.Response:
#     url = TELEGRAM_API_BASE.format(token=token, method=TELEGRAM_SENDPHOTO)
#     payload = {
#         "chat_id": chat_id,
#         "photo": photo_url,
#         "parse_mode": parse_mode
#     }
#     if caption:
#         payload["caption"] = caption[:PHOTO_CAPTION_LIMIT]
#     r = requests.post(url, data=payload, timeout=30)
#     _log_response(r)
#     return r

# def telegram_send_photo_file(token: str, chat_id: str, file_path: str, caption: Optional[str] = None, parse_mode: str = "HTML") -> requests.Response:
#     url = TELEGRAM_API_BASE.format(token=token, method=TELEGRAM_SENDPHOTO)
#     data = {"chat_id": chat_id}
#     if caption:
#         data["caption"] = caption[:PHOTO_CAPTION_LIMIT]
#         data["parse_mode"] = parse_mode
#     with open(file_path, "rb") as fh:
#         files = {"photo": fh}
#         r = requests.post(url, data=data, files=files, timeout=60)
#     _log_response(r)
#     return r

# def try_send_photo_with_fallback(token: str, chat_id: str, photo_url: str, caption: Optional[str] = None) -> bool:
#     try:
#         r = telegram_send_photo_url(token, chat_id, photo_url, caption=caption)
#         if r.status_code == 200:
#             return True
#         logger.info("Remote-photo send failed (status=%s). Attempting file upload fallback.", r.status_code)
#     except Exception as e:
#         logger.debug("Error sending photo by URL: %s", e)

#     try:
#         resp = requests.get(photo_url, stream=True, timeout=20)
#         resp.raise_for_status()
#         suffix = ""
#         ct = resp.headers.get("content-type", "")
#         if "jpeg" in ct or "jpg" in ct:
#             suffix = ".jpg"
#         elif "png" in ct:
#             suffix = ".png"
#         fd, tmp_path = tempfile.mkstemp(suffix=suffix)
#         os.close(fd)
#         with open(tmp_path, "wb") as out_f:
#             for chunk in resp.iter_content(1024 * 8):
#                 if chunk:
#                     out_f.write(chunk)
#         try:
#             r2 = telegram_send_photo_file(token, chat_id, tmp_path, caption=caption)
#             if r2.status_code == 200:
#                 try:
#                     os.remove(tmp_path)
#                 except Exception:
#                     pass
#                 return True
#         except Exception as e:
#             logger.debug("File upload fallback failed: %s", e)
#         try:
#             os.remove(tmp_path)
#         except Exception:
#             pass
#     except Exception as e:
#         logger.debug("Could not download image for fallback: %s", e)
#     return False

# # ---------- Feed reader ----------
# def read_feeds_csv(source: str) -> List[Dict[str,str]]:
#     feeds: List[Dict[str,str]] = []
#     try:
#         if source.startswith("http://") or source.startswith("https://"):
#             r = requests.get(source, timeout=30)
#             r.raise_for_status()
#             text = r.text
#             reader = csv.DictReader(text.splitlines())
#         else:
#             reader = csv.DictReader(open(source, "r", encoding="utf-8"))
#     except Exception as e:
#         logger.error("Failed to read feeds CSV %s: %s", source, e)
#         return feeds
#     for row in reader:
#         url = (row.get("feed_url") or row.get("url") or row.get("rss") or row.get("feed") or "").strip()
#         category = (row.get("category") or row.get("cat") or row.get("tag") or "").strip()
#         if url:
#             feeds.append({"url": url, "category": category})
#     return feeds

# # ---------- Duplicate logic ----------
# def is_duplicate(posted_data: Dict, entry_link: str, entry_title: str, fuzzy_threshold: int):
#     if entry_link:
#         for item in posted_data.get("items", []):
#             if item.get("link") and item["link"] == entry_link:
#                 return True, "exact_link"
#     title_norm = normalize_text(entry_title)
#     if title_norm:
#         for item in posted_data.get("items", []):
#             t = normalize_text(item.get("title", ""))
#             if not t:
#                 continue
#             score = fuzz.token_set_ratio(title_norm, t)
#             if score >= fuzzy_threshold:
#                 return True, f"fuzzy_title({score})"
#     return False, None

# # ---------- Main processing ----------
# def process(feeds_csv: str, telegram_token: str, telegram_chat: str, posted_file: str, fuzzy_threshold: int):
#     feeds = read_feeds_csv(feeds_csv)
#     logger.info("Loaded %d feeds", len(feeds))
#     posted = load_posted(posted_file)
#     new_count = 0

#     # channel name for embedding in message (attempt to show @name if provided)
#     channel_display = telegram_chat if (isinstance(telegram_chat, str) and telegram_chat.startswith("@")) else os.environ.get("TELEGRAM_CHAT_ID", telegram_chat)

#     for f in feeds:
#         url = f["url"]
#         logger.info("Checking feed: %s (category=%s)", url, f.get("category", ""))
#         try:
#             d = feedparser.parse(url)
#         except Exception as e:
#             logger.warning("feedparser failed for %s: %s", url, e)
#             continue
#         entries = d.get("entries", [])
#         for e in entries:
#             link = e.get("link") or e.get("id") or ""
#             title = e.get("title") or ""
#             published = e.get("published") or e.get("updated") or ""
#             if not link and not title:
#                 continue
#             dup, reason = is_duplicate(posted, link, title, fuzzy_threshold)
#             if dup:
#                 logger.debug("Skipping duplicate: %s (%s)", title[:120], reason)
#                 continue

#             # Try to fetch article text & image
#             top_image = None
#             summary_text = ""
#             try:
#                 if link:
#                     article = Article(link)
#                     article.download()
#                     article.parse()
#                     text = article.text or ""
#                     if text:
#                         summary_text = summarize_text(text, sentences=3)
#                         if not summary_text or len(summary_text) < 60:
#                             summary_text = first_n_sentences(text, n=2)
#                         top_image = article.top_image or None
#                     else:
#                         content = ""
#                         if "content" in e and e["content"]:
#                             content = e["content"][0].get("value", "")
#                         elif "summary" in e:
#                             content = e["summary"]
#                         summary_text = first_n_sentences(content, n=3)
#                         if e.get("media_content"):
#                             top_image = e.get("media_content", [{}])[0].get("url")
#                         elif e.get("enclosures"):
#                             top_image = e.get("enclosures", [{}])[0].get("url")
#             except Exception as ex:
#                 logger.warning("Article fetch/summarize failed for %s: %s", link, ex)
#                 summary_text = (e.get("summary") or "")[:400]

#             # Build improved Inshorts-like message (HTML)
#             # Title (bold) on its own line; category on new line; summary paragraph; source website + link; channel name at end
#             title_html = f"<b>{escape_html(title)}</b>"
#             category_html = ""
#             if f.get("category"):
#                 category_html = f"\n<i>{escape_html(f.get('category'))}</i>"

#             # extract source site name (e.g., example.com)
#             site_name = extract_site_name(link)
#             site_html = f"{escape_html(site_name)}" if site_name else ""

#             # published time (optional)
#             published_html = ""
#             if published:
#                 try:
#                     # attempt to parse published string to more compact form (best-effort)
#                     published_dt = published
#                     published_html = f"\n🕒 {escape_html(str(published_dt))}"
#                 except Exception:
#                     published_html = ""

#             summary_html = f"\n\n{escape_html(summary_text)[:800]}" if summary_text else ""

#             # Read full story link (clickable)
#             read_link_html = f'\n\n🔗 <a href="{escape_html(link)}">Read full story</a>' if link else ""

#             # Channel display (append at end)
#             channel_html = f"\n\n📣 {escape_html(str(channel_display))}" if channel_display else ""

#             # visual separators
#             top_sep = "━━━━━━━━━━━━━━━━━━━━"
#             bottom_sep = "━━━━━━━━━━━━━━━━━━━━"

#             # Full caption for photo (must be under PHOTO_CAPTION_LIMIT)
#             caption = (
#                 f"{top_sep}\n"
#                 f"{title_html}"
#                 f"{category_html}\n"
#                 f"{published_html}"
#                 f"{summary_html}"
#                 f"{read_link_html}\n"
#                 f"{channel_html}\n"
#                 f"{bottom_sep}"
#             )

#             # For text messages (sendMessage) we can use same caption (no strict limit) but keep similar formatting
#             text_message = caption

#             # Post to Telegram (try photo first if available)
#             posted_ok = False
#             try:
#                 if top_image:
#                     logger.info("Attempting to post photo by URL: %s", top_image)
#                     r = telegram_send_photo_url(telegram_token, telegram_chat, top_image, caption=caption)
#                     if r.status_code == 200:
#                         posted_ok = True
#                     else:
#                         logger.info("Photo by URL failed with status %s; trying file upload fallback", r.status_code)
#                         if try_send_photo_with_fallback(telegram_token, telegram_chat, top_image, caption=caption):
#                             posted_ok = True
#                         else:
#                             logger.info("Photo fallbacks failed; sending text message instead")
#                             r2 = telegram_send_message(telegram_token, telegram_chat, text_message)
#                             if r2.status_code == 200:
#                                 posted_ok = True
#                 else:
#                     r = telegram_send_message(telegram_token, telegram_chat, text_message)
#                     if r.status_code == 200:
#                         posted_ok = True
#             except Exception as post_ex:
#                 logger.exception("Failed to post to Telegram: %s", post_ex)

#             if posted_ok:
#                 record_posted(posted, link, title, summary=summary_text, top_image=top_image, published=published, category=f.get("category"))
#                 new_count += 1
#             else:
#                 logger.warning("Post not recorded because posting failed for: %s", title[:120])

#             time.sleep(SLEEP_BETWEEN_POSTS)

#     if new_count:
#         save_posted(posted_file, posted)
#     logger.info("Processing complete. New posts: %d", new_count)

# # ---------- CLI ----------
# def main():
#     parser = argparse.ArgumentParser()
#     parser.add_argument("--feeds-csv", default=os.environ.get("FEEDS_CSV_URL", ""), help="Feeds CSV URL or local path")
#     parser.add_argument("--posted-file", default=os.environ.get("POSTED_JSON", DEFAULT_POSTED_FILE))
#     parser.add_argument("--fuzzy-threshold", type=int, default=int_from_env("FUZZY_THRESHOLD", DEFAULT_FUZZY_THRESHOLD))
#     args = parser.parse_args()

#     TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
#     TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
#     FEEDS_CSV = args.feeds_csv

#     if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID or not FEEDS_CSV:
#         logger.error("Missing configuration. Set TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID and FEEDS_CSV_URL (or pass --feeds-csv).")
#         sys.exit(2)

#     # Ensure NLTK punkt is available (harmless if already present)
#     try:
#         import nltk
#         try:
#             nltk.data.find("tokenizers/punkt")
#         except Exception:
#             nltk.download("punkt")
#     except Exception:
#         logger.debug("NLTK not available; summarization may be degraded.")

#     process(FEEDS_CSV, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, args.posted_file, args.fuzzy_threshold)


# if __name__ == "__main__":
#     main()


#!/usr/bin/env python3
"""
rss_to_telegram.py – posts RSS items to Telegram with Inshorts-like formatting.

Features:
- Bold title on its own line
- Category in italics
- Concise summary
- Source website clickable link
- Channel name appended at the end
- Robust image posting with URL fallback to file upload
- Deduplication and posted.json logging
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
import tempfile
import urllib.parse
from datetime import datetime, timezone
from typing import Dict, Optional, List

import requests
import feedparser
from rapidfuzz import fuzz
from newspaper import Article
from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.text_rank import TextRankSummarizer

# ---------- Config ----------
DEFAULT_POSTED_FILE = "posted.json"
DEFAULT_FUZZY_THRESHOLD = 88
TELEGRAM_API_BASE = "https://api.telegram.org/bot{token}/{method}"
TELEGRAM_SENDMSG = "sendMessage"
TELEGRAM_SENDPHOTO = "sendPhoto"
SLEEP_BETWEEN_POSTS = 20
PHOTO_CAPTION_LIMIT = 1024

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------- Helpers ----------
def int_from_env(name: str, fallback: int) -> int:
    v = os.environ.get(name)
    if not v or not v.strip():
        return fallback
    try:
        return int(v)
    except Exception:
        return fallback

def sha1_text(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def normalize_text(s: Optional[str]) -> str:
    return " ".join(s.strip().lower().split()) if s else ""

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
        return out if out.strip() else first_n_sentences(text, n=2)
    except Exception as e:
        logger.debug("Summarization failed: %s", e)
        return first_n_sentences(text, n=2)

def escape_html(s: Optional[str]) -> str:
    if not s:
        return ""
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def extract_site_name(url: str) -> str:
    try:
        host = urllib.parse.urlparse(url).netloc
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""

# ---------- Persistence ----------
def load_posted(path: str) -> Dict:
    if not os.path.exists(path):
        return {"items": []}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning("Failed to load posted file %s: %s", path, e)
        return {"items": []}

def save_posted(path: str, data: Dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def record_posted(posted_data: Dict, entry_link: str, title: str, summary: str = "", top_image: Optional[str] = None, published: Optional[str] = None, category: Optional[str] = None):
    fingerprint = sha1_text((entry_link or "") + "||" + (title or ""))
    posted_data.setdefault("items", []).append({
        "link": entry_link,
        "title": title,
        "summary": summary,
        "top_image": top_image,
        "published": published,
        "category": category,
        "fingerprint": fingerprint,
        "posted_at": datetime.now(timezone.utc).isoformat()
    })

# ---------- Telegram ----------
def _log_response(r: requests.Response) -> None:
    try:
        body = r.json()
    except Exception:
        body = r.text
    logger.info("Telegram API response status=%s body=%s", r.status_code, body)

def telegram_send_message(token: str, chat_id: str, text: str, parse_mode: str = "HTML", disable_preview: bool = False) -> requests.Response:
    url = TELEGRAM_API_BASE.format(token=token, method=TELEGRAM_SENDMSG)
    payload = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode, "disable_web_page_preview": disable_preview}
    r = requests.post(url, data=payload, timeout=30)
    _log_response(r)
    return r

def telegram_send_photo_url(token: str, chat_id: str, photo_url: str, caption: Optional[str] = None, parse_mode: str = "HTML") -> requests.Response:
    url = TELEGRAM_API_BASE.format(token=token, method=TELEGRAM_SENDPHOTO)
    payload = {"chat_id": chat_id, "photo": photo_url, "parse_mode": parse_mode}
    if caption:
        payload["caption"] = caption[:PHOTO_CAPTION_LIMIT]
    r = requests.post(url, data=payload, timeout=30)
    _log_response(r)
    return r

def telegram_send_photo_file(token: str, chat_id: str, file_path: str, caption: Optional[str] = None, parse_mode: str = "HTML") -> requests.Response:
    url = TELEGRAM_API_BASE.format(token=token, method=TELEGRAM_SENDPHOTO)
    data = {"chat_id": chat_id}
    if caption:
        data["caption"] = caption[:PHOTO_CAPTION_LIMIT]
        data["parse_mode"] = parse_mode
    with open(file_path, "rb") as fh:
        files = {"photo": fh}
        r = requests.post(url, data=data, files=files, timeout=60)
    _log_response(r)
    return r

def try_send_photo_with_fallback(token: str, chat_id: str, photo_url: str, caption: Optional[str] = None) -> bool:
    try:
        r = telegram_send_photo_url(token, chat_id, photo_url, caption)
        if r.status_code == 200:
            return True
    except Exception as e:
        logger.debug("Photo URL failed: %s", e)

    try:
        resp = requests.get(photo_url, stream=True, timeout=20)
        resp.raise_for_status()
        suffix = ".jpg" if "jpeg" in resp.headers.get("content-type", "") else ".png"
        fd, tmp_path = tempfile.mkstemp(suffix=suffix)
        os.close(fd)
        with open(tmp_path, "wb") as out_f:
            for chunk in resp.iter_content(8192):
                if chunk:
                    out_f.write(chunk)
        try:
            r2 = telegram_send_photo_file(token, chat_id, tmp_path, caption)
            if r2.status_code == 200:
                os.remove(tmp_path)
                return True
        except Exception as e:
            logger.debug("File fallback failed: %s", e)
        try:
            os.remove(tmp_path)
        except Exception:
            pass
    except Exception as e:
        logger.debug("Download fallback failed: %s", e)
    return False

# ---------- Feed reader ----------
def read_feeds_csv(source: str) -> List[Dict[str,str]]:
    feeds: List[Dict[str,str]] = []
    try:
        if source.startswith("http://") or source.startswith("https://"):
            r = requests.get(source, timeout=30)
            r.raise_for_status()
            reader = csv.DictReader(r.text.splitlines())
        else:
            reader = csv.DictReader(open(source, "r", encoding="utf-8"))
    except Exception as e:
        logger.error("Failed to read feeds CSV %s: %s", source, e)
        return feeds
    for row in reader:
        url = (row.get("feed_url") or row.get("url") or row.get("rss") or row.get("feed") or "").strip()
        category = (row.get("category") or row.get("cat") or row.get("tag") or "").strip()
        if url:
            feeds.append({"url": url, "category": category})
    return feeds

# ---------- Deduplication ----------
def is_duplicate(posted_data: Dict, entry_link: str, entry_title: str, fuzzy_threshold: int):
    if entry_link:
        for item in posted_data.get("items", []):
            if item.get("link") == entry_link:
                return True, "exact_link"
    title_norm = normalize_text(entry_title)
    if title_norm:
        for item in posted_data.get("items", []):
            t = normalize_text(item.get("title", ""))
            if t and fuzz.token_set_ratio(title_norm, t) >= fuzzy_threshold:
                return True, f"fuzzy_title"
    return False, None

# ---------- Main processing ----------
def process(feeds_csv: str, telegram_token: str, telegram_chat: str, posted_file: str, fuzzy_threshold: int):
    feeds = read_feeds_csv(feeds_csv)
    logger.info("Loaded %d feeds", len(feeds))
    posted = load_posted(posted_file)
    new_count = 0

    channel_display = telegram_chat if telegram_chat.startswith("@") else os.environ.get("TELEGRAM_CHAT_ID", telegram_chat)

    for f in feeds:
        url = f["url"]
        logger.info("Checking feed: %s (category=%s)", url, f.get("category", ""))
        try:
            d = feedparser.parse(url)
        except Exception as e:
            logger.warning("feedparser failed for %s: %s", url, e)
            continue
        for e in d.get("entries", []):
            link = e.get("link") or e.get("id") or ""
            title = e.get("title") or ""
            published = e.get("published") or e.get("updated") or ""
            if not link and not title:
                continue
            dup, reason = is_duplicate(posted, link, title, fuzzy_threshold)
            if dup:
                logger.debug("Skipping duplicate: %s (%s)", title[:120], reason)
                continue

            # Fetch article text & image
            top_image = None
            summary_text = ""
            try:
                if link:
                    article = Article(link)
                    article.download()
                    article.parse()
                    text = article.text or ""
                    if text:
                        summary_text = summarize_text(text, 3)
                        if len(summary_text) < 60:
                            summary_text = first_n_sentences(text, 2)
                        top_image = article.top_image or None
                    else:
                        content = e.get("summary", "")
                        summary_text = first_n_sentences(content, 3)
                        top_image = (e.get("media_content") or e.get("enclosures") or [{}])[0].get("url")
            except Exception as ex:
                logger.warning("Article fetch failed: %s", ex)
                summary_text = (e.get("summary") or "")[:400]

            # Build message
            title_html = f"<b>{escape_html(title)}</b>"
            category_html = f"\n\n<i>{escape_html(f.get('category',''))}</i>" if f.get("category") else ""
            site_name = extract_site_name(link)
            site_html = f"{escape_html(site_name)}" if site_name else ""
            published_html = f"\n🕒 {escape_html(str(published))}" if published else ""
            summary_html = f"\n\n{escape_html(summary_text)[:800]}" if summary_text else ""
            read_link_html = f'\n\n🔗 <a href="{escape_html(link)}">Read full story</a>' if link else ""
            channel_html = f"\n\n ➡️ {escape_html(str(channel_display))}" if channel_display else ""
            

            caption = f"{title_html}\n{category_html}{published_html}\n{summary_html}\n{read_link_html}{channel_html}\n"
            text_message = caption

            posted_ok = False
            try:
                if top_image:
                    r = telegram_send_photo_url(telegram_token, telegram_chat, top_image, caption)
                    if r.status_code == 200 or try_send_photo_with_fallback(telegram_token, telegram_chat, top_image, caption):
                        posted_ok = True
                    else:
                        r2 = telegram_send_message(telegram_token, telegram_chat, text_message)
                        posted_ok = r2.status_code == 200
                else:
                    r = telegram_send_message(telegram_token, telegram_chat, text_message)
                    posted_ok = r.status_code == 200
            except Exception as ex:
                logger.exception("Posting failed: %s", ex)

            if posted_ok:
                record_posted(posted, link, title, summary=summary_text, top_image=top_image, published=published, category=f.get("category"))
                new_count += 1
            else:
                logger.warning("Post not recorded: %s", title[:120])

            time.sleep(SLEEP_BETWEEN_POSTS)

    if new_count:
        save_posted(posted_file, posted)
    logger.info("Processing complete. New posts: %d", new_count)

# ---------- CLI ----------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--feeds-csv", default=os.environ.get("FEEDS_CSV_URL", ""), help="Feeds CSV URL or local path")
    parser.add_argument("--posted-file", default=os.environ.get("POSTED_JSON", DEFAULT_POSTED_FILE))
    parser.add_argument("--fuzzy-threshold", type=int, default=int_from_env("FUZZY_THRESHOLD", DEFAULT_FUZZY_THRESHOLD))
    args = parser.parse_args()

    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
    FEEDS_CSV = args.feeds_csv

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID or not FEEDS_CSV:
        logger.error("Missing configuration. Set TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, FEEDS_CSV_URL.")
        sys.exit(2)

    # Ensure NLTK punkt tokenizer is available
    try:
        import nltk
        try:
            nltk.data.find("tokenizers/punkt")
        except Exception:
            nltk.download("punkt")
    except Exception:
        logger.debug("NLTK not available; summarization may be degraded.")

    process(FEEDS_CSV, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, args.posted_file, args.fuzzy_threshold)

if __name__ == "__main__":
    main()
