"""
Wikipedia Country Edit Stream Producer (with Wikidata Mapping)

Connects to the Wikimedia EventStream and filters for edits on country-related
pages across ALL languages. It builds a master map using Wikidata to associate
any language's article title with its corresponding 3-letter country code.
"""
import os
import requests
import sseclient
import json
import logging
import time
from dotenv import load_dotenv
from src.utils.kafka_producer import get_kafka_producer

# --- Configuration ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] - %(message)s")
logger = logging.getLogger(__name__)

# --- Constants ---
STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
CAMEO_URL = "https://www.gdeltproject.org/data/lookups/CAMEO.country.txt"
WIKIDATA_API_ENDPOINT = "https://www.wikidata.org/w/api.php"
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_WIKI_EDITS", "wiki_edits")
# Path is relative to the project root (/app in Docker)
MAP_FILE_PATH = "src/ingestion/wiki/country_master_map.json"
REQUESTS_HEADERS = {"User-Agent": "Event-Pipeline-Wiki-Global-Collector/1.0"}

def build_master_map() -> dict:
    """
    Builds a master map from every language's article title to a country code and English name.
    Example: {"대한민국": {"code": "KOR", "en_name": "South_Korea"}, ...}
    This is a slow, one-time setup process.
    """
    logger.info("🚀 Building master country map from CAMEO and Wikidata... This may take several minutes.")
    master_map = {}
    
    # 1. Fetch CAMEO country list
    response = requests.get(CAMEO_URL)
    response.raise_for_status()
    lines = response.text.strip().split('\n')[1:] # Skip header

    # 2. For each country, query Wikidata for all language sitelinks
    for line in lines:
        parts = line.split('\t')
        if len(parts) < 2: continue
        
        country_code = parts[0]
        english_name = parts[1].replace(" ", "_")
        logger.info(f"Processing {english_name} ({country_code})...")

        # Wikidata API params
        params = {
            "action": "wbgetentities", "sites": "enwiki", "titles": english_name,
            "props": "sitelinks", "format": "json", "formatversion": 2,
        }
        try:
            wd_response = requests.get(WIKIDATA_API_ENDPOINT, params=params, headers=REQUESTS_HEADERS)
            wd_response.raise_for_status()
            wd_data = wd_response.json()

            if "entities" in wd_data and wd_data["entities"]:
                entity_id = list(wd_data["entities"].keys())[0]
                if entity_id != "-1":
                    sitelinks = wd_data["entities"].get(entity_id, {}).get("sitelinks", {})
                    for sitelink in sitelinks.values():
                        # Key: article title (e.g., "대한민국"), Value: common info
                        article_title = sitelink["title"].replace(" ", "_")
                        master_map[article_title] = {"code": country_code, "en_name": english_name}
            time.sleep(0.5) # Be polite to the API
        except requests.RequestException as e:
            logger.warning(f"Could not process {english_name}: {e}")

    logger.info(f"✅ Master map built with {len(master_map)} total articles.")
    
    # 3. Cache the map to a file
    with open(MAP_FILE_PATH, 'w', encoding='utf-8') as f:
        json.dump(master_map, f, ensure_ascii=False, indent=2)
    logger.info(f"맵을 {MAP_FILE_PATH}에 저장했습니다.")
    
    return master_map

def get_master_map() -> dict:
    """Loads the master map from the cached file, or builds it if it doesn't exist."""
    if os.path.exists(MAP_FILE_PATH):
        logger.info(f"Loading master map from cached file: {MAP_FILE_PATH}")
        with open(MAP_FILE_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    else:
        return build_master_map()

def main():
    """Main function to stream filtered Wikipedia edits to Kafka."""
    try:
        master_map = get_master_map()
    except Exception as e:
        logger.critical(f"❌ Failed to build or load the master map: {e}")
        return

    logger.info(f"🚀 Starting Wikipedia Producer for Kafka topic: '{KAFKA_TOPIC}'")
    producer = get_kafka_producer()

    while True:
        try:
            response = requests.get(STREAM_URL, headers=REQUESTS_HEADERS, stream=True, timeout=300)
            response.raise_for_status()
            logger.info("✅ SSE stream connected. Filtering for edits...")
            client = sseclient.SSEClient(response)

            for event in client.events():
                if not event.data: continue
                
                try:
                    change = json.loads(event.data)
                    page_title = change.get('title', '').replace(' ', '_')

                    # --- Advanced Filtering Logic ---
                    if not change.get('bot', False) and page_title in master_map:
                        country_info = master_map[page_title]
                        
                        enriched_message = {
                            "country_code": country_info["code"],
                            "english_name": country_info["en_name"],
                            "page_title": page_title,
                            "domain": change.get('meta', {}).get('domain'),
                            "event_timestamp": change.get('meta', {}).get('dt'),
                            "user_name": change.get('user')
                        }
                        
                        producer.send(KAFKA_TOPIC, enriched_message)
                        logger.info(f"Sent edit: {page_title} ({country_info['code']})")

                except (json.JSONDecodeError, KeyError):
                    pass
        
        except Exception as e:
            logger.error(f"Stream error: {e}. Retrying in 10 seconds...")
            time.sleep(10)

if __name__ == "__main__":
    main()