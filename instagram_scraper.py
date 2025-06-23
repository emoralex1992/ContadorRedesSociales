import time
import threading
import re
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from pymongo import MongoClient
from datetime import datetime
from config import MONGODB_PASSWORD, MONGODB_DB_NAME, MONGODB_USER, MONGODB_CLUSTER

# Cuentas a seguir
INSTAGRAM_USERS = ["engi_academy"]

# MongoDB
MONGO_URI = f"mongodb+srv://{MONGODB_USER}:{MONGODB_PASSWORD}@{MONGODB_CLUSTER}/?retryWrites=true&w=majority&appName=EngiAcademyCluster"
client = MongoClient(MONGO_URI)
db = client[MONGODB_DB_NAME]
collection = db["instagram_stats"]

def init_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)

def get_followers(username):
    try:
        driver = init_driver()
        url = f"https://www.instagram.com/{username}/"
        driver.get(url)
        time.sleep(10)
        source = driver.page_source
        driver.quit()

        match = re.search(r'([\d.,]+)\s*seguidores', source, re.IGNORECASE)
        if match:
            raw = match.group(1).replace('.', '').replace(',', '')
            return int(raw)
    except Exception as e:
        print(f"Error scraping {username}: {e}")
    return None

def update_followers():
    while True:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Actualizando seguidores Instagram...")
        for user in INSTAGRAM_USERS:
            count = get_followers(user)
            if count is not None:
                collection.update_one(
                    {"username": user},
                    {"$set": {
                        "followers": count,
                        "last_updated": datetime.utcnow()
                    }},
                    upsert=True
                )
                print(f"✅ {user}: {count} seguidores")
            else:
                print(f"❌ Error obteniendo datos de {user}")
        time.sleep(300)

if __name__ == "__main__":
    thread = threading.Thread(target=update_followers, daemon=True)
    thread.start()
    input("🔁 Scraping en segundo plano. Pulsa ENTER para salir...\n")
