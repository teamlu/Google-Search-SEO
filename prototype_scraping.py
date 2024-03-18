# prototype_scraping.py
# %% 
# SETUP

# Libraries
import os
from serpapi import GoogleSearch
import pandas as pd

# Load environment variables
API_KEY = os.getenv('SERPAPI_KEY')


# Define helper functions
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
    transformed_dict = extract_organic_results(response_dict)
    
    # Format dataframe
    results_df = pd.DataFrame(transformed_dict)
    results_df['input_restaurant'] = query_restaurant
    results_df['input_address'] = query_location
    cols_of_interest = ['input_restaurant', 'input_address', 'position', 'title', 'snippet', 'snippet_highlighted_words', 'link', 'displayed_link']
    
    return results_df[cols_of_interest]


def remove_blanklisted_domains(results_data, link_column='link'):

    blacklist = [
        "mapquest", 
        "yelp", "restaurantji", "restaurantguru",
        "propertyshark", "loopnet",
        "tripadvisor", "roadtrippers",
        "slicelife", "grubhub", "doordash",
        "instagram", "facebook"
    ]
    
    def contains_keyword(link):
        return any(keyword in link for keyword in blacklist)
    
    filtered_df = results_data[~results_data[link_column].apply(contains_keyword)]
    
    return filtered_df


def strip_link_domains(results_data, link_column):

    def transform_url(link):
        stripped_protocol = link.split("//")[-1]                # Step 1: Strip protocol and anything before "//"
        stripped_www = stripped_protocol.replace("www.", "")    # Step 2: Strip "www."
        domain_only = stripped_www.split("/")[0]                # Step 3: Keep everything before the first "/"
        final_domain = ".".join(domain_only.split(".")[:2])     # Step 4: Keep the part before the first period and the subsequent string
        return final_domain
    
    results_data['stripped_domain'] = results_data[link_column].apply(transform_url)
    
    unique_domains = set(results_data['stripped_domain'])
    
    stripped_df = results_data[results_data['stripped_domain'].isin(unique_domains)].copy()
    
    return stripped_df


# %%
# SANDBOX

# Test data
q = "Blozzom Pizza"
address = "7341 Collins Avenue"
city = "Miami Beach"
full_query = q + ' ' + address

# Input selection
# df_results = get_search_results(query_restaurant=address, query_location=city)
df_results = get_search_results(query_restaurant=full_query, query_location=city)

# Link refinement
df_reduced = remove_blanklisted_domains(df_results, 'link')
df_stripped = strip_link_domains(df_reduced, 'link')

# Domain aggregation
df_aggregated = df_stripped.groupby(['input_restaurant', 'input_address'])['stripped_domain'].nunique().reset_index()

# Return restaurants with fractured online presence
df_aggregated[df_aggregated['stripped_domain'] > 1]
