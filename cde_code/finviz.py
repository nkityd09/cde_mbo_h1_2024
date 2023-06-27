# Import dependencies
import pandas as pd
from bs4 import BeautifulSoup as soup
from urllib.request import Request, urlopen
import requests
import re
from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("Write_To_S3").enableHiveSupport().getOrCreate()

s3_write_location = "s3a://ankity-cdp-aw-delete/data/" 
bucket_name = "ankity-cdp-aw-delete"
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
    except (TypeError, AttributeError):
        continue
  
  

  
#TODO Create data directory in /app/mount and add data files there

  # Create a DataFrame from the titles and string_data lists
# data = list(zip(titles, articles))
input_folder = "/app/mount/data/"

# Create the folder
os.makedirs(input_folder)
# Set the input folder path
#input_folder = "/app/mount/"

# Remove all .txt files from the input folder
txt_files = [file for file in os.listdir(input_folder) if file.endswith(".txt")]
for file in txt_files:
    file_path = os.path.join(input_folder, file)
    os.remove(file_path)
    print(f"File '{file}' removed successfully.")

print("Running Code now")
output_dir = "/app/mount/data/"


for title, data in zip(titles, articles):
    filename = title.replace(" ", "_").replace('/', '_') + ".txt"
    filepath = os.path.join(output_dir, filename)
    with open(filepath, "w") as file:
        file.write(data)
        print(f"File '{filename}' written successfully at '{filepath}'.")

# List the contents of the directory
contents = os.listdir(output_dir)

# Print the contents
for item in contents:
    print(item)


print(os.path.abspath(output_dir+"Three_Best_Practices_for_Making_Lasting_Life_Changes.txt"))

df = spark.read.text(output_dir+"Three_Best_Practices_for_Making_Lasting_Life_Changes.txt")
df.show()


# # Read each text file from the input folder as a DataFrame
# file_paths = spark.sparkContext.wholeTextFiles(input_folder).keys().collect()
# data = spark.read.text(file_paths)

# # Define the UDF function
# extract_filename = udf(lambda path: path.split("/")[-1], StringType())

# # Extract the filename using the UDF
# data = data.withColumn("filename", extract_filename(data["value"]))
# print(data)
# data.show()

# Generate the S3 output path for each file
#output_paths = data.select(concat(lit("data"), "filename").alias("output_path"))

# Write each file to S3
# data = data.withColumn("file_content", data["value"])
# data = data.select("filename", "file_content")

# data.write.text("s3a://" + bucket_name + "/temp")