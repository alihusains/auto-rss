import argparse
import csv
import json
import os
import sys
import requests
import feedparser
from newspaper import Article
from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.textrank import TextRankSummarizer
from rapidfuzz import fuzz

POSTED_FILE_DEFAULT = "posted.json"
FUZZY_THRESHOLD_DEFAULT = 88

def load_posted(posted_file):
    if os.path.exists(posted_file):
        with open(posted_file, "r") as f:
            return json.load(f)
    return []

def save_posted(posted_file, posted):
    with open(posted_file, "w") as f:
        json.dump(posted, f)

def read_feeds(csv_url):
    feeds = []
    resp = requests.get(csv_url)
    resp.raise_for_status()
    reader = csv.DictReader(resp.text.splitlines())
    for row in reader:
        feeds.append({"category": row["Category"], "url": row["URL"]})
    return feeds

def summarize_text(text):
    parser = PlaintextParser.from_string(text, Tokenizer("english"))
    summarizer = TextRankSummarizer()
    summary_sentences = summarizer(parser.document, sentences_count=2)
    return " ".join(str(s) for s in summary_sentences)

def post_to_telegram(bot_token, chat_id, message):
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    resp = requests.post(url, data={"chat_id": chat_id, "text": message, "disable_web_page_preview": False})
    resp.raise_for_status()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--feeds-csv", required=True)
    parser.add_argument("--posted-file", default=POSTED_FILE_DEFAULT)
    parser.add_argument("--fuzzy-threshold", type=int, default=FUZZY_THRESHOLD_DEFAULT)
    args = parser.parse_args()

    posted = load_posted(args.posted_file)
    feeds = read_feeds(args.feeds_csv)

    new_posted = posted.copy()

    for feed in feeds:
        parsed_feed = feedparser.parse(feed["url"])
        for entry in parsed_feed.entries:
            title = entry.get("title", "")
            link = entry.get("link", "")
            content = entry.get("summary", "") or entry.get("description", "")

            # Skip duplicate by exact URL
            if any(p["link"] == link for p in posted):
                continue

            # Skip duplicates by fuzzy title match
            if any(fuzz.ratio(p["title"], title) > args.fuzzy_threshold for p in posted):
                continue

            # Fetch full text if possible
            try:
                article = Article(link)
                article.download()
                article.parse()
                content_text = article.text or content
            except:
                content_text = content

            summary = summarize_text(content_text)
            message = f"*{title}*\n{summary}\n{link}"

            try:
                post_to_telegram(os.environ["TELEGRAM_BOT_TOKEN"], os.environ["TELEGRAM_CHAT_ID"], message)
                print(f"Posted: {title}")
            except Exception as e:
                print(f"Failed to post: {title}, error: {e}", file=sys.stderr)

            new_posted.append({"title": title, "link": link})

    save_posted(args.posted_file, new_posted)

if __name__ == "__main__":
    main()
