# 🎬 YouTube Shorts Comment Scraper

A powerful and practical scraper for extracting comments from YouTube Shorts videos using Selenium and undetected-chromedriver.

This tool is designed to handle dynamic content loading, expand all replies, and collect both top-level and nested comments. It supports batch scraping across multiple Shorts URLs.

---

## 🚀 Features

- ✅ Scrape **YouTube Shorts comments**
- ✅ Support **batch processing** of multiple video URLs
- ✅ Extract:
  - Comment text
  - Author
  - Likes
  - Relative & absolute timestamps
  - Replies (nested comments)
- ✅ Automatically:
  - Scroll to load all comments
  - Expand all replies
- ✅ Built-in:
  - Anti-detection (undetected-chromedriver)
  - Proxy support
  - Retry mechanism
- ✅ Export data to **CSV**

---

## ⚠️ Important Note (Manual Interaction Required)

Due to YouTube Shorts' UI design, the comment panel must be opened manually.

During execution, the program will prompt you to:

1. Open the Shorts video page
2. Click the **comment button (💬 icon)**
3. Press ENTER in the terminal to continue

This step ensures the scraper can access the comment section properly.

---

## 🧰 Requirements

- Python 3.8+
- Google Chrome (installed)

### Install dependencies:

```bash
pip install undetected-chromedriver selenium pandas requests
