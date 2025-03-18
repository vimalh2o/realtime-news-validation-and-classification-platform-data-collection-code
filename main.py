# Install Required Libraries
#!pip install requests beautifulsoup4 pymongo schedule pytz spacy faiss-cpu numpy sentence-transformers
#!python -m spacy download en_core_web_sm


import subprocess
import sys

# Ensure the Spacy model is downloaded
try:
    import spacy
    nlp = spacy.load("en_core_web_sm")
except OSError:
    print("Downloading Spacy model: en_core_web_sm...")
    subprocess.run([sys.executable, "-m", "spacy", "download", "en_core_web_sm"], check=True)
    nlp = spacy.load("en_core_web_sm")

# Import Required Libraries
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pytz
import time
import pymongo
import schedule
import faiss
import numpy as np
from sentence_transformers import SentenceTransformer

# Load the Sentence Transformer model for embeddings
embed_model = SentenceTransformer('all-MiniLM-L6-v2')

# MongoDB Setup
mongo_client = pymongo.MongoClient("mongodb+srv://vimal:vimal@cluster0.2678f.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = mongo_client['news_db']
news_collection = db['news_data']


# Function to insert data into MongoDB
def insert_data_to_mongodb(news_data):
    if news_data:
        for news_item in news_data:
            # Check for duplicates before inserting
            if news_collection.count_documents({'link': news_item['link']}) == 0:
                news_collection.insert_one(news_item)
        print("Data inserted into MongoDB successfully!")
    else:
        print("No data to insert.")

# Function to generate embeddings
def generate_embedding(text):
    return embed_model.encode(text, convert_to_numpy=True).tolist()

# Function to extract named entities from content
def extract_named_entities(content):
    doc = nlp(content)
    entities = {
        "persons": list(set([ent.text for ent in doc.ents if ent.label_ == "PERSON"])),
        "organizations": list(set([ent.text for ent in doc.ents if ent.label_ == "ORG"])),
        "places": list(set([ent.text for ent in doc.ents if ent.label_ == "GPE"]))
    }
    return entities


# Function to process and store news
def process_and_store_news(news_data):
    for news in news_data:
        news['embedding'] = generate_embedding(news['content'])
    insert_data_to_mongodb(news_data)

# Scraper 1: GNews API
def fetch_news_from_api(api_url, api_key):
    response = requests.get(api_url, params={'token': api_key, 'lang': 'en'})
    if response.status_code != 200:
        print(f"Failed to fetch data from API: {response.status_code}")
        return []
    data = response.json()
    articles = data.get('articles', [])
    news_data = []
    for article in articles:
        title, link, content = article.get('title', ''), article.get('url', ''), article.get('content', '')
        published_at = article.get('publishedAt', '')
        source = article.get('source', {}).get('name', '')
        try:
            date = datetime.strptime(published_at, '%Y-%m-%dT%H:%M:%SZ').strftime('%b %d, %Y')
        except ValueError:
            date = 'Invalid date format'
        insertion_time = datetime.now(pytz.timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S')
        entities = extract_named_entities(content)
        news_data.append({'title': title, 'link': link, 'content': content, 'date': date, 'source': source, 'insertion_time': insertion_time, 'entities': entities})
    return news_data

# Scraper 2: India Today
def fetch_news_from_india_today():
    url = 'https://www.indiatoday.in/latest-news'
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    news_items = []
    for article in soup.find_all('div', class_='B1S3_content__wrap__9mSB6'):
        title_tag = article.find('h2')
        title = title_tag.text.strip() if title_tag else 'No title'
        link_tag = article.find('a', href=True)
        link = link_tag['href'] if link_tag else ''
        if not link.startswith('http'):
            link = 'https://www.indiatoday.in' + link
        try:
            article_response = requests.get(link)
            article_soup = BeautifulSoup(article_response.content, 'html.parser')
            content = ' '.join(p.get_text(strip=True) for p in article_soup.find_all('p'))
            date_tag = article_soup.find('span', class_='jsx-ace90f4eca22afc7 strydate')
            published_date = date_tag.text.strip() if date_tag else "No date"
        except:
            content, published_date = "Error fetching content", "Error fetching date"
        insertion_time = datetime.now(pytz.timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S')
        entities = extract_named_entities(content)
        news_items.append({'title': title, 'link': link, 'content': content, 'date': published_date, 'source': 'India Today', 'insertion_time': insertion_time, 'entities': entities})
    return news_items



# Scraper 3 & 4: Times of India & Hindustan Times (same structure as above, omitted for brevity)
# Scraper 3: Times of India
def scrape_times_of_india():
    base_url = "https://timesofindia.indiatimes.com/news"
    response = requests.get(base_url)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")

    articles_data = []
    articles = soup.select("ul > li > a")
    for article in articles:
        title_element = article.select_one("div > p")
        if not title_element:
            continue
        title = title_element.text.strip()

        link = article.get("href")
        if not link.startswith("http"):
            link = f"https://timesofindia.indiatimes.com{link}"

        try:
            article_response = requests.get(link)
            article_soup = BeautifulSoup(article_response.content, "html.parser")

            content_element = article_soup.select_one("div._s30J.clearfix")
            content = content_element.text.strip() if content_element else "Content not available"

            date_element = article_soup.select_one("div.xf8Pm.byline > span")
            date_published = date_element.text.strip() if date_element else "Date not available"
        except Exception as e:
            content = "Error fetching content"
            date_published = "Error fetching date"

        insertion_time = datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S")

        # Extract NER (named entities)
        entities = extract_named_entities(content)

        articles_data.append({
            "title": title,
            "link": link,
            "content": content,
            "date": date_published,
            "source": "Times of India",
            "insertion_time": insertion_time,
            'entities': entities  # Add NER data
        })

    return articles_data

# Scraper 4: Hindustan Times
def scrape_hindustan_times():
    base_url = "https://www.hindustantimes.com/latest-news"
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(base_url, headers=headers)
    soup = BeautifulSoup(response.content, "html.parser")

    news_items = []
    links = soup.select("a.storyLink.articleClick")
    for article in links:
        link = article.get("href")
        if not link.startswith("http"):
            link = f"https://www.hindustantimes.com{link}"

        try:
            article_response = requests.get(link, headers=headers)
            article_soup = BeautifulSoup(article_response.content, "html.parser")

            title = article_soup.select_one("h1.hdg1").get_text(strip=True) if article_soup.select_one("h1.hdg1") else "No title"
            content = " ".join(p.get_text(strip=True) for p in article_soup.select("div.detail p")) or "Content not available"
            published_time = article_soup.select_one("div.dateTime.secTime.storyPage")
            published_date = published_time.get_text(strip=True) if published_time else "No date available"
        except Exception as e:
            content = "Error fetching content"
            published_date = "Error fetching date"

        insertion_time = datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S")

        # Extract NER (named entities)
        entities = extract_named_entities(content)

        news_items.append({
            "title": title,
            "link": link,
            "content": content,
            "date": published_date,
            "source": "Hindustan Times",
            "insertion_time": insertion_time,
            'entities': entities  # Add NER data
        })

    return news_items
# Unified function to collect news
def collect_and_store_news():
    api_url = 'https://gnews.io/api/v4/top-headlines'
    api_key = '329bc462aed7eca21aed0983ea6f7a08'
    print("Fetching news from API...")
    api_news = fetch_news_from_api(api_url, api_key)
    print("Fetching news from India Today...")
    india_today_news = fetch_news_from_india_today()
    print("Fetching news from Times of India...")
    times_of_india_news = scrape_times_of_india()  # Add scraping function here
    print("Fetching news from Hindustan Times...")
    hindustan_times_news = scrape_hindustan_times()  # Add scraping function here
    all_news = api_news + india_today_news + times_of_india_news + hindustan_times_news
    process_and_store_news(all_news)

# Scheduling


def run_scraper():
    start_time = time.time()
    
    print(f"Starting news collection at {datetime.now(pytz.timezone('Asia/Kolkata'))}...")
    collect_and_store_news()  # Run your scraper

    run_duration = time.time() - start_time  # Calculate execution time
    print(f"Script executed in {run_duration:.2f} seconds. Exiting...")

if __name__ == "__main__":
    run_scraper()  # Runs once and exits
