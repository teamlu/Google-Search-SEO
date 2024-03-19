# run_batch.py

# Libraries
import os
from dotenv import load_dotenv
import logging

from src.api_manager import SerpAPI
from src.helpers import (
    batch_process
)

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def main():
    
    logging.info("Starting batch execution.")
    
    # Get environment vars
    API_KEY = os.getenv('SERPAPI_KEY')
    CSV_PATH = os.getenv('CSV_PATH')
    
    # Initialize api manager
    scraper_api = SerpAPI(API_KEY)
    
    # Request desired restaurant count to run
    input_sample_n = input("Enter number of restaurants: ")
    
    # Convert input_sample_n to integer
    sample_n = int(input_sample_n)
    
    # Run the batch processing with the path to the CSV and the initialized SerpAPI instance
    data = batch_process(CSV_PATH, scraper_api, sample_n)
    print(data.to_json(orient="records", lines=True, indent=4))    
    
if __name__ == '__main__':
    main()
    
