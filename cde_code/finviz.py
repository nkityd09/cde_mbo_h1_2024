# Import dependencies
import pandas as pd
from bs4 import BeautifulSoup as soup
from urllib.request import Request, urlopen
import requests

# Set up scraper
url = "https://finviz.com/news.ashx"
req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
webpage = urlopen(req).read()
html = soup(webpage, "html.parser")


# print(type(html))
# # Define function to scrape and process news
# def scrape_news(html, idx):
#     try:
#         news = pd.read_html(str(html))[idx]
#         #news.columns = ["0", "Time", "Headlines"]
#         #news = news.drop(columns=["0"])
#         #news = news.set_index("Time")
#         return news
#     except Exception as e:
#         print(f"Error: {e}")
#         return None


# # Scrape and show general news
# news_df = scrape_news(html, 5)
# if news_df is not None:
#     print("\nGeneral News: ")
#     print(news_df)

# # Scrape and show blog news
# blog_news_df = scrape_news(html, 6)
# if blog_news_df is not None:
#     print("\nBlog News: ")
#     print(blog_news_df)



entries = html.find_all("tr", class_="nn")
#print(entries)



entry = html.find("tr", class_="nn")
print(entry)
article_body = html.find("td", class_="news_tooltip-tab")
print("######")
print(article_body)

# response = requests.get(link)
# article_soup = soup(response.text, "html.parser")
# article_body = article_soup.find("td", class_="news_tooltip-tab")

# if article_body:
#     article_body_text = article_body.text.strip()
#     print(article_body_text)
# else:
#     print("Unable to find article body.")
