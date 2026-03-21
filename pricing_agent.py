import json
import os
import time
import uuid

import requests
from kafka import KafkaConsumer, KafkaProducer

import production.book_utils as bk
import production.kaneru_io as io
import production.book_pricer as book_pricer
from production.kaneru_book_category import latent_price_by_embedding
from datetime import datetime

FINANCIALS_GATEWAY_URL = "http://localhost:8881"

_APPLICATION_ID = os.getenv("APPLICATION_ID", "kaneru_seller").replace("_", "-")
_KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
_PRICE_REQUEST_TOPIC = f"{_APPLICATION_ID}.price-search-request.books"
_PRICE_RESPONSE_TOPIC = f"{_APPLICATION_ID}.price-search-response.books"
_EXPECTED_SOURCES = {"abe", "ebay"}
_PRICE_SEARCH_TIMEOUT = 30  # seconds to wait for all responses


def convert_ccy_price(price, price_ccy, target_ccy):
    """Legacy shim — calls financials_gateway /financials/convert."""
    try:
        res = requests.post(
            f"{FINANCIALS_GATEWAY_URL}/financials/convert",
            json={"price": price, "from_ccy": price_ccy, "to_ccy": target_ccy},
            timeout=10,
        )
        data = res.json()
        if data.get("status") == "ok":
            return data["data"]["converted_price"]
    except Exception as e:
        print(f"financials_gateway convert failed: {e}")
    return None


def kafka_book_search(title, subtitle, author):
    """Publish a price search request to Kafka and collect responses from all services."""
    rid = str(uuid.uuid4())

    # Start consumer BEFORE publishing so we don't miss responses
    consumer_group = f"pricing_agent_{rid}"
    consumer = KafkaConsumer(
        _PRICE_RESPONSE_TOPIC,
        bootstrap_servers=_KAFKA_BOOTSTRAP,
        group_id=consumer_group,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        consumer_timeout_ms=(_PRICE_SEARCH_TIMEOUT * 1000),
    )
    # Force partition assignment
    consumer.poll(timeout_ms=2000)

    # Publish request
    try:
        producer = KafkaProducer(
            bootstrap_servers=_KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        producer.send(_PRICE_REQUEST_TOPIC, value={
            "rid": rid,
            "title": title,
            "subtitle": subtitle or "",
            "author": author or "",
        })
        producer.flush()
        producer.close()
    except Exception as e:
        print(f"kafka publish failed: {e}")
        consumer.close()
        return []

    # Collect responses until all sources reply or timeout
    all_results = []
    sources_received = set()
    deadline = time.time() + _PRICE_SEARCH_TIMEOUT

    try:
        for message in consumer:
            val = message.value
            if val.get("rid") != rid:
                continue

            source = val.get("price_source", "unknown")
            status = val.get("status")
            data = val.get("data") or []

            if status == "ok":
                all_results.extend(data)
                print(f"price search | rid={rid} | source={source} | results={len(data)}")
            else:
                print(f"price search | rid={rid} | source={source} | error={val.get('message')}")

            sources_received.add(source)
            if sources_received >= _EXPECTED_SOURCES:
                break

            if time.time() >= deadline:
                missing = _EXPECTED_SOURCES - sources_received
                print(f"price search timeout | rid={rid} | missing={missing}")
                break
    except Exception as e:
        print(f"kafka consume error: {e}")
    finally:
        consumer.close()

    return all_results

def convert_to_usd(book_data):

    price = book_data['price']
    price_ccy = book_data['ccy_code']
    target_ccy = "USD"
    usd_price = convert_ccy_price(price, price_ccy, target_ccy)
    book_data['price'] = usd_price
    book_data['ccy_code'] = "USD"
    return book_data

def price_get_latent_price(title, subtitle, author, isbn13):

    if subtitle is None:
        subtitle = ''

    if isbn13 is None or isbn13=='':
        book_id = bk.generate_inventory_id(title.lower() + subtitle.lower() + author.lower()) 
    else:
        book_id = bk.generate_inventory_id(isbn13)
    
    is_cached, latent_price = io.is_latent_price_cached(book_id) 
    
    if is_cached and latent_price!=0:
        print(f"was cached: {latent_price}", flush=True)
        return True

    print("Issuing price search")
    book_prices = kafka_book_search(title, subtitle, author)
    book_prices = [convert_to_usd(book_data) for book_data in book_prices]
    
    print(book_prices)
    
    if len(book_prices) == 0:
        print(f"No book data available for: {title}: {subtitle}, {author}", flush=True)
        latent_price = 0
    else:
        current_year = datetime.today().year
        latent_price = book_pricer.estimate_latent(current_year, book_prices, isbn13)
        if latent_price is None:
            print(f"No book data available for: {title}: {subtitle}, {author}", flush=True) 
            latent_price = 0
    
    if latent_price !=0:  
        status = io.store_latent_price(book_id, latent_price) 
    else:
        status = False
    return status

def get_book_price(book):

    print("################# Starting Pricer #############################")

    title = book['title']
    subtitle = book['subtitle']
    if subtitle is None:
        subtitle = ''
    author = '' if book['author'] is None else book['author']
    isbn_13 = book['isbn_13']

    if isbn_13 is None or isbn_13=='':
        variant_tag=""
        book_id = bk.generate_inventory_id(title.lower() + subtitle.lower() + author.lower()) 
    else:
        book_id = bk.generate_inventory_id(isbn_13)

    current_year = datetime.today().year

    is_price_cached, latent_price = io.is_latent_price_cached(book_id)
    
    if not is_price_cached:
        #TODO put the latent pricer back once we get the price scrapper hooked up properly again
        latent_price = None #latent_price_by_embedding(book_id)
        if latent_price is None:
            print("embedding failed")
            status = price_get_latent_price(title, subtitle, author, isbn_13)
            if status:
                _, latent_price = io.is_latent_price_cached(book_id) 
    
    if latent_price is None:
        print("Failed to determine price")
        return None
        
    print(latent_price)    
        
    book_price = book_pricer.estimate(current_year, latent_price, book)
      
    return book_price



