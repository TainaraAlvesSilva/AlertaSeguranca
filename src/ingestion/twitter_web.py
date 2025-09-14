# src/ingestion/twitter_web.py
from typing import List, Dict
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time

def _scrape(url: str, limit: int) -> List[Dict]:
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.get(url)
    time.sleep(5)  

    data: List[Dict] = []

    # Faz scroll para carregar mais tweets
    last_height = driver.execute_script("return document.body.scrollHeight")
    while len(data) < limit:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break  # fim da página
        last_height = new_height

        tweets = driver.find_elements(By.XPATH, '//article')
        for t in tweets[len(data):limit]:
            try:
                author = t.find_element(By.XPATH, './/div[@data-testid="User-Name"]').text
            except:
                author = None

            try:
                text = t.find_element(By.XPATH, './/div[@data-testid="tweetText"]').text
            except:
                text = ""

            try:
                published = t.find_element(By.TAG_NAME, "time").get_attribute("datetime")
            except:
                published = None

            try:
                like_count = t.find_element(By.XPATH, './/div[@data-testid="like"]').text or "0"
            except:
                like_count = "0"

            try:
                retweet_count = t.find_element(By.XPATH, './/div[@data-testid="retweet"]').text or "0"
            except:
                retweet_count = "0"

            try:
                reply_count = t.find_element(By.XPATH, './/div[@data-testid="reply"]').text or "0"
            except:
                reply_count = "0"

            try:
                permalink = t.find_element(By.XPATH, ".//a[contains(@href, '/status/')]").get_attribute("href")
            except:
                permalink = None

            data.append({
                "author": author,
                "text": text,
                "publishedAt": published,
                "likeCount": like_count,
                "retweetCount": retweet_count,
                "replyCount": reply_count,
                "permalink": permalink,
            })

    if not data:
        print("[WARN] Nenhum tweet carregado!")
        with open("debug_twitter.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)

    driver.quit()
    return data
def scrape_twitter_profile(username: str, limit: int = 10) -> List[Dict]:
    """Raspa tweets de um perfil específico."""
    url = f"https://twitter.com/{username}"
    return _scrape(url, limit)


def scrape_twitter_hashtag(hashtag: str, limit: int = 10) -> List[Dict]:
    """Raspa tweets de uma hashtag específica."""
    url = f"https://twitter.com/hashtag/{hashtag}"
    return _scrape(url, limit)


def scrape_twitter_post(url: str, limit: int = 10) -> List[Dict]:
    """Raspa respostas de um post específico (URL completa do tweet)."""
    return _scrape(url, limit)
