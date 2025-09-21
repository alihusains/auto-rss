#!/usr/bin/env python3
"""
rss_to_telegram.py

Reads a CSV of RSS feeds (published Google Sheet CSV URL),
parses feeds, deduplicates, extracts article text & image,
summarizes, and posts to a Telegram channel via Bot API.

Configuration via environment variables (preferred for secrets):
 - TELEGRAM_BOT_TOKEN  (required)  -> Telegram Bot token from BotFather
 - TELEGRAM_CHAT_ID    (required)  -> channel id (e.g. -1001234567890) or @channelusername
 - FEEDS_CSV_URL       (required)  -> published Google Sheet CSV URL (set as secret)
 - POSTED_JSON         (optional)  -> path to state file (default: posted.json)
 - FUZZY_THRESHOLD     (optional)  -> integer (0-100), default 88
 - SLEEP_BETWEEN_POSTS (optional)  -> seconds to sleep between posts, default 1.0

Notes:
 - This script persists posted items to POSTED_JSON; when used in GitHub Actions,
   the workflow commits that file back to the repo so state persists between runs.
 - For local runs you can use a .env file (not committed) or set env vars in your shell.
"""

import os
import csv
import sys
import json
import time
import hashlib
import logging
import argparse
from datetime import datetime, timezone

import requests
import feedparser
from rapidfuzz import fuzz
from newspaper import Article
from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.textrank import TextRankSummarizer

# --------- CONFIG DEFAULTS -------------------------------------------------
DEFAULT_POSTED_FILE = 'posted.json'
DEFAULT_FUZZY_THRESHOLD = 88  # 0-100
DEFAULT_SLEEP = 1.0
TELEGRAM_API_BASE = 'https://api.telegram.org/bot{token}/{method}'
TELEGRAM_SENDMSG = 'sendMessage'
TELEGRAM_SENDPHOTO = 'sendPhoto'

# --------- Logging ---------------------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')


# --------- Utilities -------------------------------------------------------
def load_posted(path):
    if not os.path.exists(path):
        return {'items': [], 'meta': {}}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {'items': [], 'meta': {}}


def save_posted(path, data):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def sha1_text(s):
    return hashlib.sha1(s.encode('utf-8')).hexdigest()


def normalize_text(s):
    if not s:
        return ''
    return ' '.join(s.strip().lower().split())


def first_n_sentences(text, n=2):
    if not text:
        return ''
    # naive split — fine for short summaries
    parts = [p.strip() for p in text.split('.') if p.strip()]
    return '. '.join(parts[:n]) + ('.' if len(parts[:n]) > 0 else '')


def summarize_text(text, sentences=3):
    try:
        parser = PlaintextParser.from_string(text, Tokenizer('english'))
        summarizer = TextRankSummarizer()
        summary_sentences = summarizer(parser.document, sentences)
        return ' '.join([str(s) for s in summary_sentences])
    except Exception as e:
        logging.warning('Summarization failed: %s', e)
        return first_n_sentences(text, n=2)


def escape_html(s):
    if not s:
        return ''
    return s.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')


def telegram_send_message(token, chat_id, text, parse_mode='HTML', disable_web_page_preview=False):
    url = TELEGRAM_API_BASE.format(token=token, method=TELEGRAM_SENDMSG)
    payload = {
        'chat_id': chat_id,
        'text': text,
        'parse_mode': parse_mode,
        'disable_web_page_preview': disable_web_page_preview
    }
    r = requests.post(url, data=payload, timeout=20)
    logging.info('Telegram sendMessage status=%s', r.status_code)
    return r


def telegram_send_photo(token, chat_id, photo_url, caption=None, parse_mode='HTML'):
    url = TELEGRAM_API_BASE.format(token=token, method=TELEGRAM_SENDPHOTO)
    payload = {
        'chat_id': chat_id,
        'photo': photo_url,
        'parse_mode': parse_mode
    }
    if caption:
        payload['caption'] = caption[:1000]  # Telegram caption limit safety
    r = requests.post(url, data=payload, timeout=30)
    logging.info('Telegram sendPhoto status=%s', r.status_code)
    return r


def fetch_article(url):
    art = Article(url)
    try:
        art.download()
        art.parse()
    except Exception as e:
        logging.warning('newspaper download/parse failed for %s: %s', url, e)
        return None
    try:
        art.nlp()
    except Exception:
        pass
    return {
        'text': art.text or '',
        'top_image': art.top_image or None,
        'title': art.title or None
    }


def read_feeds_csv(source):
    feeds = []
    if source.startswith('http://') or source.startswith('https://'):
        r = requests.get(source, timeout=30)
        r.raise_for_status()
        text = r.text
        reader = csv.DictReader(text.splitlines())
    else:
        reader = csv.DictReader(open(source, 'r', encoding='utf-8'))
    for row in reader:
        url = (row.get('feed_url') or row.get('url') or row.get('rss') or row.get('feed') or '').strip()
        category = (row.get('category') or row.get('cat') or row.get('tag') or '').strip()
        if url:
            feeds.append({'url': url, 'category': category})
    return feeds


def is_duplicate(posted_data, entry_link, entry_title, fuzzy_threshold=DEFAULT_FUZZY_THRESHOLD):
    # exact link check
    for item in posted_data.get('items', []):
        if item.get('link') and item['link'] == entry_link:
            return True, 'exact_link'
    # fuzzy title check
    title_norm = normalize_text(entry_title)
    if not title_norm:
        return False, None
    for item in posted_data.get('items', []):
        t = normalize_text(item.get('title', ''))
        if not t:
            continue
        score = fuzz.token_set_ratio(title_norm, t)
        if score >= fuzzy_threshold:
            return True, f'fuzzy_title({score})'
    return False, None


def record_posted(posted_data, entry_link, title, fingerprint=None):
    if not fingerprint:
        fingerprint = sha1_text((entry_link or '') + '||' + (title or ''))
    posted_data.setdefault('items', []).append({
        'link': entry_link,
        'title': title,
        'fingerprint': fingerprint,
        'posted_at': datetime.now(timezone.utc).isoformat()
    })


def process(feeds_csv, telegram_token, telegram_chat, posted_file, fuzzy_threshold, sleep_between_posts):
    feeds = read_feeds_csv(feeds_csv)
    logging.info('Read %d feeds', len(feeds))
    posted = load_posted(posted_file)
    new_posts = []

    for f in feeds:
        logging.info('Checking feed: %s (category=%s)', f['url'], f.get('category', ''))
        try:
            d = feedparser.parse(f['url'])
        except Exception as e:
            logging.warning('feedparser failed for %s: %s', f['url'], e)
            continue
        entries = d.get('entries', [])
        for e in entries:
            link = e.get('link') or e.get('id') or ''
            title = e.get('title') or ''
            if not link and not title:
                continue
            dup, reason = is_duplicate(posted, link, title, fuzzy_threshold)
            if dup:
                logging.info('Skipping duplicate: "%s" (%s)', (title[:120] + '...') if len(title) > 120 else title, reason)
                continue

            # Attempt to fetch article and summarize
            article = None
            summary = ''
            top_image = None
            try:
                if link:
                    article = fetch_article(link)
                if article and article.get('text'):
                    summary = summarize_text(article['text'], sentences=3)
                    if not summary or len(summary) < 80:
                        summary = first_n_sentences(article['text'], n=2)
                    top_image = article.get('top_image')
                else:
                    # fallback to feed content/summary
                    content = ''
                    if 'content' in e and e['content']:
                        content = e['content'][0].get('value', '')
                    elif 'summary' in e:
                        content = e['summary']
                    summary = first_n_sentences(content, n=3)
                    # attempt to extract image from media/enclosure
                    if e.get('media_content'):
                        top_image = e.get('media_content', [{}])[0].get('url')
                    elif e.get('enclosures'):
                        top_image = e.get('enclosures', [{}])[0].get('url')

            except Exception as ex:
                logging.warning('Article fetch/summarize failed for %s: %s', link, ex)
                summary = (e.get('summary') or '')[:400]

            # prepare HTML caption/message
            title_html = f"<b>{escape_html(title)}</b>"
            cat_html = f"<i>{escape_html(f.get('category',''))}</i>" if f.get('category') else ''
            source_html = f"\n\nSource: <a href=\"{escape_html(link)}\">Read original</a>" if link else ''
            body_html = escape_html(summary or '')[:900]  # cap for caption safety
            caption = title_html + "\n" + body_html + source_html

            # send to Telegram
            try:
                if top_image:
                    logging.info('Posting as photo to telegram: %s', link)
                    r = telegram_send_photo(telegram_token, telegram_chat, top_image, caption=caption)
                    if r.status_code != 200:
                        logging.warning('sendPhoto failed (status=%s), fallback to sendMessage', r.status_code)
                        telegram_send_message(telegram_token, telegram_chat, title_html + "\n" + body_html + source_html)
                else:
                    logging.info('Posting as text to telegram: %s', link)
                    telegram_send_message(telegram_token, telegram_chat, title_html + "\n" + body_html + source_html)

                # record posted
                record_posted(posted, link, title)
                new_posts.append({'link': link, 'title': title})
                time.sleep(sleep_between_posts)
            except Exception as post_err:
                logging.exception('Failed to post to telegram: %s', post_err)

    # persist posted.json if new posts exist
    if new_posts:
        save_posted(posted_file, posted)
        logging.info('Saved %d new posts to %s', len(new_posts), posted_file)
    else:
        logging.info('No new posts found.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--feeds-csv', default=os.environ.get('FEEDS_CSV_URL', ''), help='Feeds CSV URL or local path (default from FEEDS_CSV_URL env)')
    parser.add_argument('--posted-file', default=os.environ.get('POSTED_JSON', DEFAULT_POSTED_FILE))
    parser.add_argument('--fuzzy-threshold', type=int, default=int(os.environ.get('FUZZY_THRESHOLD', DEFAULT_FUZZY_THRESHOLD)))
    parser.add_argument('--sleep-between-posts', type=float, default=float(os.environ.get('SLEEP_BETWEEN_POSTS', DEFAULT_SLEEP)))
    args = parser.parse_args()

    TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
    TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
    FEEDS_CSV = args.feeds_csv

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID or not FEEDS_CSV:
        logging.error('Missing required environment variables. Ensure TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, and FEEDS_CSV_URL (or --feeds-csv) are set.')
        sys.exit(2)

    process(FEEDS_CSV, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, args.posted_file, args.fuzzy_threshold, args.sleep_between_posts)
