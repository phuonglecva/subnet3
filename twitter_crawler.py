from concurrent.futures import ThreadPoolExecutor
import pprint
import json
import sqlite3
from pprint import pprint
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
import time
import datetime
import re
import random
import traceback

from webdriver_manager.chrome import ChromeDriverManager

from selenium.webdriver.support.wait import WebDriverWait 
from selenium.webdriver.support import expected_conditions as EC

from neurons.storage.sqlite_miner_storage import SqliteMinerStorage
# storage = SqliteMinerStorage("/home/ubuntu/wspace/data-universe/SqliteMinerStorage.sqlite")
class TwitterCrawler:
    STEP_HEIGHT = 50_000
    MAX_SECONDS_PER_EACH_ROUND = 1_000
    
    def __init__(self, auth_token, hashtags=[]) -> None:
        self.hashtags = hashtags
        self.auth_token = auth_token
        self.write_times = 0
        self.file_path = "twitter_links.txt"
        self.path = "SqliteMinerStorage.sqlite"
        self.driver = self.get_driver()
        self.load_cookies()
        self.result = []
        self.extracted_acticles = []
        self.cutoff_date = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=30)
        self.storage = SqliteMinerStorage()

    def get_driver(self):
        options = webdriver.ChromeOptions()
        service = webdriver.ChromeService(executable_path=ChromeDriverManager().install())
        options.add_argument("--window-size=2560,1440")
        # options.add_argument("--headless=new")
        # options.add_argument("--no-sandbox")
        # options.add_argument("--disable-gpu")
        options.add_argument("--incognito")
        options.add_argument("--disable-cookie-encryption")
        options.add_extension("old-twitter.crx")
        driver = webdriver.Chrome(options=options, service=service)
        return driver
    
    def extract_aritcle_info(self, article):
        # time.sleep(1000)
        content = article.find_element(
            By.CSS_SELECTOR, "div[data-testid='tweetText']")
        print(f"content: {content.text}")
        dtime = article.find_element(
            By.CSS_SELECTOR, "time")
        print(f"datetime: {dtime.get_attribute('datetime')}")
        hastags = article.find_elements(By.CSS_SELECTOR, "a[href*='/hashtag']")
        print(f"hashtags: {hastags}")
        user_elem = article.find_element(
            By.CSS_SELECTOR, "div[data-testid='User-Name']")
        print(f"user: {user_elem.text}")
        like_elem = article.find_element(
            By.CSS_SELECTOR, "div[data-testid='like']")
        print(f"likes: {like_elem.text}")
        images = article.find_elements(
            By.CSS_SELECTOR, "img[alt='Image']")
        image_urls = [img.get_attribute("src") for img in images if img.get_attribute("src") != ""]
        print(f"images: {image_urls}")
        url = article.find_element(
            By.CSS_SELECTOR, "a[href*='/status']")
        print(f"url: {url}")
        content = {
            'model_config': {'extra': 'ignore'},
            'text': content.text,
            'timestamp': datetime.datetime.strptime(dtime.get_attribute("datetime"), "%Y-%m-%dT%H:%M:%S.%fZ").timestamp(),
            'tweet_hashtags': [h.text for h in hastags if h != ""],
            'url': url.get_attribute("href"),
            'username': user_elem.text,
            "likes": like_elem.text,
            "images": image_urls[0] if len(image_urls) > 0 else ""
        }
        return content
    
    def load_new_hashtag(self, hashtag):
        self.driver.get(f"https://twitter.com/search?q={hashtag}")
        time.sleep(3)
        
    def switch_to_live_page(self):
        print(f"switched to live mode: {self.current_hashtag}")
        self.driver.get(f"https://twitter.com/search?q={self.current_hashtag}&f=live")
        time.sleep(5)

    def get_current_height(self):
        return self.driver.execute_script(
            "return document.body.scrollHeight;")

    def refresh_page(self):
        self.driver.close()
        self.driver = self.get_driver()
                
    def scan_all(self):
        for (i, hashtag) in enumerate(self.hashtags):
            try:
                self.current_hashtag = hashtag
                print(f"start scan {hashtag}")
                self.load_new_hashtag(hashtag)
                self.scroll_and_get_links()
                print(f"scanned {hashtag} hashtags")
                self.switch_to_live_page()            
                self.scroll_and_get_links(True)
                print(f"scanned {hashtag} live mode")
                # self.refresh_page()
            except Exception as e:
                traceback.print_exc()
                print("something error")
                self.check_and_save()
                self.refresh_page()
                continue
    def start(self):
        while True:
            self.hashtags = random.sample(self.hashtags, len(self.hashtags))
            print(f"start scan all")
            self.scan_all()
            time.sleep(3)
            print(f"start write result")
            
    def scroll_and_get_links(self, live=False):
        print(f"tag: {self.current_hashtag} init height: {self.get_current_height()}")
        times = 5 if live is False else 10
        for _ in range(times):
            self.driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)
        # WebDriverWait(self.driver, 25).until(
        #     EC.presence_of_element_located((By.CSS_SELECTOR, "article[data-testid='tweet']")))
        time.sleep(5)
        articles = self.driver.find_elements(
            By.CSS_SELECTOR, "article[data-testid='tweet']")
        
        if articles is None or len(articles) == 0:
            print("no articles")
            self.refresh_page()
            return
        for article in articles:
            try:
                info =  self.extract_aritcle_info(article)
            except Exception as e:
                print("error when extracting article")
                continue
            if datetime.datetime.fromtimestamp(info['timestamp'], tz=datetime.timezone.utc).timestamp() < self.cutoff_date.timestamp():
                print("too old")
                continue
            self.extracted_acticles.append({
                "url": info["url"],
                "search_query": self.current_hashtag,
                "text": info["text"],
                "likes": info["likes"],
                "images": info["images"],
                "username": info["username"],
                "hashtags": ",".join(info["tweet_hashtags"]),
                "timestamp": info["timestamp"],
            })
        self.check_and_save()
    
    def check_and_save(self):
        print(f"extracted {len(self.extracted_acticles)} articles.")
        if len(self.extracted_acticles) > 0:
            self.storage.insert_records(self.extracted_acticles)
            print(f"stored {len(self.extracted_acticles)} data")
            self.extracted_acticles = []
        
    def load_cookies(self):
        self.driver.get("https://twitter.com")
        self.driver.add_cookie({
            "name": "auth_token",
            "value": self.auth_token,
            "path": "/",
            "domain": "twitter.com"
        })
        time.sleep(2)

if __name__=='__main__':
    auth_tokens = [
            "b82bb8969cb084350f8b1def33613b54cf409a9c",
        ]
    with open("keywords.txt", "r") as f:
        hashtags = f.readlines()
        hashtags = [hashtag.strip() for hashtag in hashtags]
    print(len(hashtags))
    partial_len = len(hashtags) // len(auth_tokens)
    from concurrent.futures import ThreadPoolExecutor
    executor = ThreadPoolExecutor(max_workers=10)
    tasks = []
    for i, auth_token in enumerate(auth_tokens):
        from_idx = i * partial_len
        to_indx = (i + 1) * partial_len
        partial_hts = hashtags[from_idx: to_indx]
        crawler = TwitterCrawler(auth_token, partial_hts)
        tasks.append(executor.submit(crawler.start))
    
    for task in tasks:
        task.result()
