python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
python3 -c "import nltk; nltk.download('punkt')"

# set env vars for a quick local test (do not commit secrets)
export TELEGRAM_BOT_TOKEN="8402495596:AAFeHv1rMtpZ8dg38pFziVltvx9uS-ughN4"
export TELEGRAM_CHAT_ID="@shianewsofficial"   # or -100123...
# export FEEDS_CSV_URL="https://docs.google.com/spreadsheets/d/1igRFrUYNWrotaIFpwFxiza8ccJbXBHnddfNlEppu_iw/export?output=csv&format=csv"

export FEEDS_CSV_URL="https://docs.google.com/spreadsheets/d/e/2PACX-1vQZdh3lmxUJY3p9NSQabJg7LGSjrKq8__CWbfv1LcKTTI1FFks8xIbsukouKHGWJCbt2lvxmGbWhTrP/pub?gid=437548596&single=true&output=csv"

python3 rss_to_telegram.py --feeds-csv "$FEEDS_CSV_URL"
