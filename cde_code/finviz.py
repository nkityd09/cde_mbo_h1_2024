# Import dependencies
import pandas as pd
from bs4 import BeautifulSoup as soup
from urllib.request import Request, urlopen
import requests
import re
from pyspark.sql import SparkSession
import os

spark = SparkSession\
        .builder\
        .appName("Write_To_S3")\
        .enableHiveSupport()\
        .getOrCreate()
s3_write_location = "s3a://ankity-cdp-aw-delete/data/" 

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
# print(titles)  
# print(links)


subset_links = links[0:10]


for link in subset_links:
  req = Request(link, headers={"User-Agent": "Mozilla/5.0"})
  webpage = urlopen(req).read()
  html = soup(webpage, "html.parser")
  paragraphs = html.find_all('p')

  # Extract the text from each paragraph
  paragraph_texts = [paragraph.get_text() for paragraph in paragraphs]
  merged_article = ' '.join(paragraph_texts)
  articles.append(merged_article)
  

  # Create a DataFrame from the titles and string_data lists
data = list(zip(titles, articles))
df = spark.createDataFrame(data, ["title", "data"])

for row in df.collect():
    title = row["title"]
    data = row["data"]
    file_name = title.replace(" ", "_") + ".txt"
    s3_path = s3_write_location + file_name
    with open(file_name, "w") as file:
        file.write(data)
    spark.sparkContext.parallelize([file_name]).coalesce(1).saveAsTextFile(s3_path)

# # Function to write each file to S3
# def write_to_s3(row):
#     title = row["title"]
#     data = row["data"]
#     file_name = title + ".txt"
#     s3_path = s3_write_location + file_name
    
#     # Write data to a local file
#     with open(file_name, "w") as file:
#         file.write(data)
    
#     # Write the local file to S3
#     spark.read.text(file_name).write.text(s3_path)
    
#     # Clean up the local file
#     os.remove(file_name)

# # Apply the write_to_s3 function to each row of the DataFrame
# df.foreach(write_to_s3)