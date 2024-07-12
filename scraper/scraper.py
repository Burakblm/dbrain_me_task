import requests
from confluent_kafka import Producer
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import os
import json
import time
import threading
from queue import Queue

load_dotenv()

WEBSITE_URL = os.getenv('WEBSITE_URL')
KAFKA_BROKER = os.getenv("KAFKA_BROKER")

conf = {
    'bootstrap.servers': KAFKA_BROKER
}

producer = Producer(conf)
product_queue = Queue()

def scrape_products():
    while True:
        response = requests.get(WEBSITE_URL)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            product_links = [a['href'] for a in soup.find_all('a', class_='woocommerce-LoopProduct-link')]

            for link in product_links:
                product_response = requests.get(link)
                if product_response.status_code == 200:
                    product_soup = BeautifulSoup(product_response.content, 'html.parser')
                    name = product_soup.find('h1', class_='product_title').text.strip()
                    description = product_soup.find('div', class_='woocommerce-product-details__short-description').text.strip()
                    stock = product_soup.find('p', class_='stock').text.strip()
                    price = product_soup.find('p', class_='price').text.strip()

                    data = {
                        'name': name,
                        'price': price,
                        'description': description,
                        'stock': stock,
                    }
                    product_queue.put(data)


def send_to_kafka():
    while True:
        if not product_queue.empty():
            products = []
            while not product_queue.empty():
                products.append(product_queue.get())
            
            for product in products:
                producer.produce('pokemon-topic', key='key', value=json.dumps(product))
            producer.flush()
        time.sleep(1)

scraping_thread = threading.Thread(target=scrape_products)
scraping_thread.daemon = True
scraping_thread.start()

sending_thread = threading.Thread(target=send_to_kafka)
sending_thread.daemon = True
sending_thread.start()

while True:
    time.sleep(1)
