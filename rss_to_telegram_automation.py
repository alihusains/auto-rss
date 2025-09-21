# README + Automation files (single-file view)
# This document contains three artifacts in one place:
# 1) `rss_to_telegram.py` - the main Python script
# 2) `requirements.txt` - Python dependencies
# 3) `.github/workflows/rss-to-telegram.yml` - GitHub Actions workflow

# ---------------------------------------------------------------------------
# FILE 1: rss_to_telegram.py
# ---------------------------------------------------------------------------
"""
rss_to_telegram.py
A self-contained script that:
 - reads a CSV (published Google Sheet URL or local file) containing RSS feed URLs and categories
 - parses feeds with feedparser
 - detects new items (compares against posted.json stored in repo)
 - deduplicates across feeds using exact-link + fuzzy-title matching (RapidFuzz)
 - fetches article text & top image with newspaper3k
 - creates a short extractive summary (Sumy) or fallbacks to first 2 sentences
 - posts to a Telegram public channel (sendPhoto or sendMessage) using the Bot API
 - updates posted.json and commits it back to the repo (used inside GitHub Actions)

CONFIG (via environment variables or CLI args):
 - TELEGRAM_BOT_TOKEN  (env / secret)
 - TELEGRAM_CHAT_ID    (env / secret)  e.g. "@yourchannel" or -1001234567890
 - FEEDS_CSV_URL       (env / arg)     e.g. link returned by Google Sheets "Publish to web" (output=csv)
 - FUZZY_THRESHOLD     (optional, default 88)
 - POSTED_JSON         path to posted.json (default ./posted.json)

Notes:
 - This script is tailored to run inside GitHub Actions (it commits posted.json back to repo)
 - If you prefer Google Apps Script approach, skip the commit bits and persist state into a sheet.

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
DEFAULT_FUZZY_THRESHOLD = 88  # 0-100, higher = more strict (adjustable)
TELEGRAM_API_BASE = 'https://api.telegram.org/bot{token}/{method}'
TELEGRAM_SENDMSG = 'sendMessage'
TELEGRAM_SENDPHOTO = 'sendPhoto'

# --------- UTILITIES -------------------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')


def load_posted(path):
    if not os.path.exists(path):
        return {
            'items': [],
            'meta': {}
        }
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_posted(path, data):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def sha1_text(s):
    return hashlib.sha1(s.encode('utf-8')).hexdigest()


def normalize_text(s):
    if not s:
        return ''
    return ' '.join(s.strip().lower().split())

# extract first n sentences as fallback

def first_n_sentences(text, n=2):
    # naive sentence split by period — works well enough for short summaries
    parts = [p.strip() for p in text.split('.') if p.strip()]
    return '. '.join(parts[:n]) + ('.' if len(parts[:n])>0 else '')

# summarization using Sumy (TextRank) - extractive

def summarize_text(text, sentences=3):
    try:
        parser = PlaintextParser.from_string(text, Tokenizer('english'))
        summarizer = TextRankSummarizer()
        summary_sentences = summarizer(parser.document, sentences)
        return ' '.join([str(s) for s in summary_sentences])
    except Exception as e:
        logging.warning('Sumy summarization failed: %s', e)
        return first_n_sentences(text, n=2)

# safe HTML escape for Telegram (we use simple replacements for <>&)

def escape_html(s):
    if not s:
        return ''
    return s.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

# telegram send helpers

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
        payload['caption'] = caption[:1000]  # keep inside Telegram caption limits
    r = requests.post(url, data=payload, timeout=30)
    logging.info('Telegram sendPhoto status=%s', r.status_code)
    return r

# fetch article via newspaper3k

def fetch_article(url):
    art = Article(url)
    try:
        art.download()
        art.parse()
    except Exception as e:
        logging.warning('newspaper download/parse failed for %s: %s', url, e)
        return None
    # attempt nlp for summary if available
    try:
        art.nlp()
    except Exception:
        # nlp() may fail if punkt not available; it's ok
        pass
    return {
        'text': art.text or '',
        'top_image': art.top_image or None,
        'title': art.title or None
    }

# read CSV of feeds (feed_url,category)

def read_feeds_csv(source):
    feeds = []
    if source.startswith('http://') or source.startswith('https://'):
        r = requests.get(source, timeout=20)
        r.raise_for_status()
        text = r.text
        reader = csv.DictReader(text.splitlines())
    else:
        reader = csv.DictReader(open(source, 'r', encoding='utf-8'))
    for row in reader:
        url = row.get('feed_url') or row.get('url') or row.get('rss') or row.get('feed')
        category = row.get('category') or row.get('cat') or row.get('tag') or ''
        if url:
            feeds.append({'url': url.strip(), 'category': category.strip()})
    return feeds

# dedup checks

def is_duplicate(posted_data, entry_link, entry_title, fuzzy_threshold=DEFAULT_FUZZY_THRESHOLD):
    # exact link check
    for item in posted_data['items']:
        if 'link' in item and item['link'] == entry_link:
            return True, 'exact_link'
    # fuzzy title check against recent items
    title_norm = normalize_text(entry_title)
    for item in posted_data['items']:
        t = normalize_text(item.get('title',''))
        if not t or not title_norm:
            continue
        score = fuzz.token_set_ratio(title_norm, t)
        if score >= fuzzy_threshold:
            return True, f'fuzzy_title({score})'
    return False, None

# record posted item

def record_posted(posted_data, entry_link, title, fingerprint=None):
    if not fingerprint:
        fingerprint = sha1_text((entry_link or '') + '||' + (title or ''))
    posted_data['items'].append({
        'link': entry_link,
        'title': title,
        'fingerprint': fingerprint,
        'posted_at': datetime.now(timezone.utc).isoformat()
    })

# main processing loop

def process(feeds_csv, telegram_token, telegram_chat, posted_file, fuzzy_threshold):
    feeds = read_feeds_csv(feeds_csv)
    logging.info('Read %d feeds', len(feeds))
    posted = load_posted(posted_file)
    new_posts = []

    for f in feeds:
        logging.info('Checking feed: %s', f['url'])
        try:
            d = feedparser.parse(f['url'])
        except Exception as e:
            logging.warning('feedparser failed for %s: %s', f['url'], e)
            continue
        entries = d.get('entries', [])
        for e in entries:
            link = e.get('link') or e.get('id') or ''
            title = e.get('title') or ''
            published = e.get('published') or e.get('updated') or ''
            # dedup
            dup, reason = is_duplicate(posted, link, title, fuzzy_threshold)
            if dup:
                logging.info('Skipping duplicate: %s (%s)', title, reason)
                continue
            # fetch article text & image
            article = None
            summary = ''
            top_image = None
            try:
                article = fetch_article(link)
                if article and article.get('text'):
                    # try extractive summarization
                    summary = summarize_text(article['text'], sentences=3)
                    if not summary or len(summary) < 80:
                        summary = first_n_sentences(article['text'], n=2)
                    top_image = article.get('top_image')
                else:
                    # fallback to content from feed (if exists)
                    content = ''
                    if 'content' in e and e['content']:
                        content = e['content'][0].get('value','')
                    elif 'summary' in e:
                        content = e['summary']
                    summary = first_n_sentences(content, n=3)
                    # try to pick image from media:content or enclosure
                    top_image = e.get('media_content', [{}])[0].get('url') if e.get('media_content') else e.get('enclosures',[{}])[0].get('url')
            except Exception as ex:
                logging.warning('Article fetch/summarize failed: %s', ex)
                summary = e.get('summary','')[:400]

            # prepare message
            title_html = f"<b>{escape_html(title)}</b>"
            cat_html = f"<i>{escape_html(f.get('category',''))}</i>" if f.get('category') else ''
            source_html = f"\n\nSource: <a href=\"{escape_html(link)}\">Read original</a>"
            caption = title_html + '\n' + escape_html(summary)[:900] + source_html

            # send to telegram
            try:
                if top_image:
                    logging.info('Posting as photo to telegram: %s', link)
                    r = telegram_send_photo(telegram_token, telegram_chat, top_image, caption=caption)
                    if r.status_code != 200:
                        logging.warning('sendPhoto failed, fallback to sendMessage status=%s', r.status_code)
                        telegram_send_message(telegram_token, telegram_chat, title_html + '\n' + escape_html(summary) + source_html)
                else:
                    logging.info('Posting as text to telegram: %s', link)
                    telegram_send_message(telegram_token, telegram_chat, title_html + '\n' + escape_html(summary) + source_html)
                # record posted
                record_posted(posted, link, title)
                new_posts.append({'link': link, 'title': title})
                # be polite – small sleep to avoid flooding
                time.sleep(1.0)
            except Exception as post_err:
                logging.exception('Failed to post to telegram: %s', post_err)

    # save posted.json
    if new_posts:
        save_posted(posted_file, posted)
        logging.info('Saved %d new posts to %s', len(new_posts), posted_file)
    else:
        logging.info('No new posts found.')


# ------------ CLI / ENTRYPOINT ---------------------------------------------
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--feeds-csv', dest='feeds_csv', default=os.environ.get('FEEDS_CSV_URL','feeds.csv'))
    parser.add_argument('--posted-file', dest='posted_file', default=os.environ.get('POSTED_JSON', DEFAULT_POSTED_FILE))
    parser.add_argument('--fuzzy-threshold', dest='fuzzy_threshold', type=int, default=int(os.environ.get('FUZZY_THRESHOLD', DEFAULT_FUZZY_THRESHOLD)))
    args = parser.parse_args()

    TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
    TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.error('Please set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID as environment variables.')
        sys.exit(2)

    process(args.feeds_csv, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, args.posted_file, args.fuzzy_threshold)

# ---------------------------------------------------------------------------
# FILE 2: requirements.txt
# ---------------------------------------------------------------------------
# Put this in requirements.txt (exact versions are optional)
# feedparser, newspaper3k, requests, sumy, rapidfuzz
# Rapidfuzz provides fast fuzzy matching. Newspaper3k extracts article text & top image.

# requirements.txt
# ----------------
# feedparser
# newspaper3k
# requests
# sumy
# rapidfuzz
# nltk

# Note: newspaper3k and sumy may need small NLTK downloads (nltk.download('punkt'))

# ---------------------------------------------------------------------------
# FILE 3: .github/workflows/rss-to-telegram.yml
# ---------------------------------------------------------------------------
# GitHub Actions workflow (place under .github/workflows/rss-to-telegram.yml)
# - scheduled run (every 5 minutes) + manual dispatch
# - commits updated posted.json back to repo (permissions: contents: write required)

workflow_yaml = r"""
name: RSS -> Telegram channel

on:
  schedule:
    - cron: '*/5 * * * *'   # every 5 minutes (shortest allowed); adjust as you like
  workflow_dispatch: {}

permissions:
  contents: write

jobs:
  run:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4
        with:
          persist-credentials: true

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt
          python - <<'PY'
import nltk
nltk.download('punkt')
PY

      - name: Run RSS -> Telegram script
        env:
          TELEGRAM_BOT_TOKEN: ${{ secrets.TELEGRAM_BOT_TOKEN }}
          TELEGRAM_CHAT_ID: ${{ secrets.TELEGRAM_CHAT_ID }}
          FEEDS_CSV_URL: ${{ secrets.FEEDS_CSV_URL }} # optional - or pass as file
        run: |
          python rss_to_telegram.py --feeds-csv "$FEEDS_CSV_URL"

      - name: Commit posted.json (if changed)
        run: |
          git config --global user.email "action@github.com"
          git config --global user.name "github-actions[bot]"
          git add posted.json || true
          if ! git diff --staged --quiet; then
            git commit -m "Update posted.json (new articles)" || true
            git push
          else
            echo "No changes to commit"
          fi
"""

# Write workflow to file so the user can copy/paste easily (not executed here)

print('\n--- COPY the Python script, requirements.txt and workflow YAML into your repo. ---\n')
print('\nFiles included in this canvas:\n - rss_to_telegram.py\n - requirements.txt (commented above)\n - .github/workflows/rss-to-telegram.yml (embedded in this document)\n')

# End of document
