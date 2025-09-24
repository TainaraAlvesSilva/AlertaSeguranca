import time
from typing import List, Dict, Optional
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

def _scrape(user: str, password: str, url: str, limit: int) -> List[Dict]:
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    
    # Login Instagram
    driver.get("https://www.instagram.com/accounts/login/")
    time.sleep(2)
    driver.find_elements(By.NAME, "username")[0].send_keys(user)
    driver.find_elements(By.NAME, "password")[0].send_keys(password)
    driver.find_elements(By.XPATH, "//button[@type='submit']")[0].click()
    time.sleep(5)

    driver.get(url)
    time.sleep(5)

    # scroller do painel de comentários
    scroller = driver.find_element(
        By.XPATH,
        "//div[contains(@class,'x5yr21d') and contains(@class,'xw2csxc') "
        "and contains(@class,'x1odjw0f') and contains(@class,'x1n2onr6')]"
    )

    data: List[Dict] = []

    # helpers
    def _get_author(c) -> Optional[str]:
        try:
          # Procura o primeiro link <a> com atributo href que começa com "/" - ex: "/usuario/"
            a = c.find_element(By.XPATH, ".//a[starts-with(@href,'/') and not(contains(@href,'/p/'))][1]")
            try:
              # Procura um span com a tag '_ap3a'
                return a.find_element(By.XPATH, ".//span[contains(@class,'_ap3a')]").text.strip()
            except:
              # Se não encontar o span, pega o valor do href
                href = a.get_attribute("href") or ""
                return href.strip("/").split("/")[-1] if href else None
        except:
            pass

        try:
          # Pega qualquer span com a tag '_ap3a'
            return c.find_element(By.XPATH, ".//span[contains(@class,'_ap3a')]").text.strip()
        except:
            return None

    def _get_comment(c) -> str:
      # Procura um span que não esteja dentro de um <a></a>
        try:
            return c.find_element(By.XPATH, ".//div[contains(@class,'xdt5ytf') and contains(@class,'x1cy8zhl')]//span[normalize-space()!=''][1]").text.strip()
        except:
            return ""

    def _get_datetime(c) -> Optional[str]:
        try:
            return c.find_element(By.TAG_NAME, "time").get_attribute("datetime")
        except:
            return None

    # scrolla o painel de comentários
    last_height = driver.execute_script("return arguments[0].scrollHeight", scroller)
    while len(data) < limit:
        driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight;", scroller)
        time.sleep(3)
        new_height = driver.execute_script("return arguments[0].scrollHeight", scroller)
        if new_height == last_height:
            break # fim da página
        last_height = new_height

        # pega os blocos de comentários
        comments = scroller.find_elements(
          By.XPATH,
          ".//a[contains(@href,'/p/') and contains(@href,'/c/')]/time"
          "/ancestor::div[.//div[contains(@class,'xdt5ytf') and contains(@class,'x1cy8zhl')]][1]"
        )

        for c in comments[len(data):limit]:
            author = _get_author(c)
            comment = _get_comment(c)
            published = _get_datetime(c)

            if not comment:
                continue

            data.append({
                "author": author,
                "comment": comment,
                "publishedAt": published
            })
            
    if not data:
        print("[WARN] Nenhum comentário coletado!")
        with open("debug_instagram.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)

    driver.quit()
    return data

def scrape_instagram_post(user: str, password: str, id: str, limit: int = 10) -> List[Dict]:
    url = f"https://www.instagram.com/p/{id}"
    return _scrape(user, password, url, limit)

def scrape_instagram_reels(user: str, password: str, id: str, limit: int = 10) -> List[Dict]:
    url = f"https://www.instagram.com/reel/{id}"
    return _scrape(user, password, url, limit)