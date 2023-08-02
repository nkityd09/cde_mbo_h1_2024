# Import dependencies
import pandas as pd
from bs4 import BeautifulSoup as soup
from urllib.request import Request, urlopen
import requests
import urllib.error
import re
from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import current_date, current_timestamp
from datetime import date, datetime

spark = SparkSession.builder.appName("Write_To_S3").enableHiveSupport().getOrCreate()

s3_write_location = "s3a://ankity-cdp-aw-delete/data/" 
bucket_name = "ankity-cdp-aw-delete"

current_date_str = date.today()
now = datetime.now()
current_time = now.strftime("%H-%M-%S")
folder_path = f"s3a://{bucket_name}/data/date={current_date_str}/timestamp={current_time}/"


# Set up scraper
url = "https://finviz.com/news.ashx"
req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
webpage = urlopen(req).read()
html = soup(webpage, "html.parser")


news_items = []
for row in html.find_all("tr", class_="nn"):
    date = row.find("td", class_="nn-date").text.strip()
    link = row.find("a", class_="nn-tab-link")
    title = row.find("a", class_="nn-tab-link")
    source_icon = row.find("td", class_=re.compile("news_source_icon")).find("use")
    news_items.append({"date": date, "link": link, "title": title, "source_icon": source_icon})

titles = []
links = []
articles = []

for item in news_items:
    try:
        link_html = item['link']
        link = link_html.get("href")
        links.append(link)
        title_html = item['title']
        title = [tag.text for tag in title_html]
        titles.append(title[0])
    except (TypeError, AttributeError):
        continue




for link in links:
    try:
        req = Request(link, headers={"User-Agent": "Mozilla/5.0"})
        webpage = urlopen(req).read()
        html = soup(webpage, "html.parser")
        paragraphs = html.find_all('p')

        # Extract the text from each paragraph
        paragraph_texts = [paragraph.get_text() for paragraph in paragraphs]
        merged_article = ' '.join(paragraph_texts)
        articles.append(merged_article)
    except (TypeError, AttributeError, urllib.error.HTTPError):
        continue
  
# Combine the lists into tuples
data = list(zip(titles, links, articles))

# Create a DataFrame with column names
df = spark.createDataFrame(data, ["title", "link", "article_text"])
df.coalesce(1).write.option("header", "true").parquet(f"{folder_path}")




