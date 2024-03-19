# run_single_record.py

# Libraries
import os
from dotenv import load_dotenv

from src.api_manager import SerpAPI
from src.helpers import (
    remove_blacklisted_domains,
    remove_correlated_domains,
    remove_location_domains,
    remove_low_similarity_domains
)

load_dotenv()


def main():
    
    # Get environment vars
    API_KEY = os.getenv('SERPAPI_KEY')

    # Initialize api manager
    scraper_api = SerpAPI(API_KEY)
    
    # Inputs
    # - Niki's Pizza & Pasta - Cedar Park
    # - 508 North Bell Boulevard
    # - Cedar Park
    input_account_name = input("Enter account name: ")
    input_billing_address = input("Enter billing address: ")
    input_billing_city = input("Enter billing city: ")
    
    # Create search query, combining restaurant name and address
    full_query = input_account_name + ' ' + input_billing_address

    # Input selection
    df_results = scraper_api.get_search_results(query_restaurant=full_query, query_location=input_billing_city)

    # Filter noisy results
    df_reduced = remove_blacklisted_domains(df_results, 'link')
    df_stripped = remove_correlated_domains(df_reduced, 'link')
    df_subset = remove_location_domains(df_stripped)

    # Aggregate domain names
    df_aggregated = df_subset.groupby(['input_restaurant', 'input_city'])['stripped_domain'].agg(lambda x: list(set(x))).reset_index()
    df_aggregated.rename(columns={'stripped_domain': 'unique_domain_list'}, inplace=True)

    # Determine restaurants with fractured online presence
    df_fractured = df_aggregated[df_aggregated['unique_domain_list'].apply(lambda x: len(x) > 1)]
    df_fractured_with_similarity = remove_low_similarity_domains(df_fractured)
    df_fractured_with_similarity['unique_domain_count'] = df_aggregated['unique_domain_list'].apply(len)
    
    # Show results
    print("All unique domains: ", list(df_fractured_with_similarity['unique_domain_list']))
    print("Number of unique domains: ", df_fractured_with_similarity['unique_domain_count'])
    
if __name__ == '__main__':
    main()

