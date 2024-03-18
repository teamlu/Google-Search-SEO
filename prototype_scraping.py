# prototype_scraping.py
# %% 
import os
from serpapi import GoogleSearch
import pandas as pd

API_KEY = os.getenv('SERPAPI_KEY')

def extract_organic_results(response_dict):

    organic_results = response_dict.get("organic_results", [])
    if not organic_results:
        return []

    results_data = []
    keys_to_extract = organic_results[0].keys()
    
    for result in organic_results:
        result_data = {key: result.get(key, None) for key in keys_to_extract}
        results_data.append(result_data)
    
    return results_data

def get_search_results(query_restaurant, query_location=None):
    # Set payload
    params = {
        "q": query_restaurant,
        "hl": "en",
        "gl": "us",
        "google_domain": "google.com",
        "api_key": API_KEY
    }    
    if query_location:
        params["location"] = query_location

    # Execute search
    response_raw = GoogleSearch(params)
    response_dict = response_raw.get_dict()
    
    # Parse nested json
    data = extract_organic_results(response_dict)
    
    # Format dataframe
    df_organic_results = pd.DataFrame(data)
    df_organic_results['input_restaurant'] = query_restaurant
    df_organic_results['input_address'] = query_location
    
    cols_of_interest = ['input_restaurant', 'input_address', 'position', 'title', 'snippet', 'snippet_highlighted_words', 'link', 'displayed_link']
    
    return df_organic_results[cols_of_interest]


# TESTING
q = "Blozzom Pizza"
address = "7341 Collins Avenue"
city = "Miami Beach"

full_query = q + ' ' + address

df_results = get_search_results(query_restaurant=full_query, query_location=city)
# df_results = get_search_results(query_restaurant=address, query_location=city)
df_results

# %%
