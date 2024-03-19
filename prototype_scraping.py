# %% 
# SETUP

# Libraries
import os
from serpapi import GoogleSearch
import pandas as pd

# Load environment variables
API_KEY = os.getenv('SERPAPI_KEY')


# Define api functions
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


def get_search_results(query_restaurant, 
                       query_location=None):
    
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


# Define helper functions


def remove_blacklisted_domains(results_data, link_column='link'):

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


def remove_correlated_domains(results_data, link_column):

    def strip_domain(link):
        stripped_protocol = link.split("//")[-1]                # Step 1: Strip protocol and anything before "//"
        stripped_www = stripped_protocol.replace("www.", "")    # Step 2: Strip "www."
        domain_only = stripped_www.split("/")[0]                # Step 3: Keep everything before the first "/"
        final_domain = ".".join(domain_only.split(".")[:2])     # Step 4: Keep the part before the first period and the subsequent string
        return final_domain
    
    results_data['stripped_domain'] = results_data[link_column].apply(strip_domain)
    
    unique_domains = set(results_data['stripped_domain'])
    
    stripped_df = results_data[results_data['stripped_domain'].isin(unique_domains)].copy()
    
    return stripped_df


def remove_location_domains(results_data):
    
    def clean_token(token):
        """Remove non-alphanumeric characters and lowercase a token for uniform comparison."""
        return ''.join(char for char in token if char.isalnum()).lower()

    def token_in_domain(token, domain):
        """Determine if a token is a substring of any part of a domain, facilitating broad match searches."""
        cleaned_token = clean_token(token)
        domain_tokens = domain.split('.')
        return any(cleaned_token in clean_token(domain_token) for domain_token in domain_tokens)

    def all_address_tokens_in_domain(address_tokens, domain):
        """Checks that address-related tokens can be found within the domain, identifying potential address-focused sites."""
        return all(token_in_domain(token, domain) for token in address_tokens)

    def no_refined_restaurant_tokens_in_domain(refined_restaurant_tokens, domain):
        """Checks that restaurant tokens don't appear in the domain, aiming to exclude non-relevant or overly general sites."""
        return all(not token_in_domain(token, domain) for token in refined_restaurant_tokens)

    indices_to_keep = []

    for index, row in results_data.iterrows():
        restaurant_tokens = {clean_token(token) for token in row['input_restaurant'].lower().split()}
        address_tokens = {clean_token(token) for token in row['input_address'].lower().split()}
        domain = row['stripped_domain'].lower()

        # Remove address tokens from restaurant tokens
        refined_restaurant_tokens = restaurant_tokens - address_tokens

        # print("Restaurant: ", restaurant_tokens)
        # print("Address: ", address_tokens)
        # print("Domain: ", domain)
        # print("Refined Restaurant: ", refined_restaurant_tokens)

        if all_address_tokens_in_domain(address_tokens, domain) and no_refined_restaurant_tokens_in_domain(refined_restaurant_tokens, domain):
            continue
        else:
            indices_to_keep.append(index)

    return results_data.loc[indices_to_keep]


# %%
# SANDBOX

# Test 1
q = "Blozzom Pizza"
address = "7341 Collins Avenue"
city = "Miami Beach"

# Test 2
q = "Paradise Pizza & Grill"
address = "400 N Main St"
city = "Southington"	

# Test 3
q = "Niki's Pizza & Pasta - Cedar Park"
address = "508 North Bell Boulevard"
city = "Cedar Park"	

# Create search query, combining restaurant name and address
full_query = q + ' ' + address

# Input selection
# df_results = get_search_results(query_restaurant=address, query_location=city)
df_results = get_search_results(query_restaurant=full_query, query_location=city)

# Link refinement
df_reduced = remove_blacklisted_domains(df_results, 'link')
df_stripped = remove_correlated_domains(df_reduced, 'link')
df_subset = remove_location_domains(df_stripped)

# Domain aggregation
df_aggregated = df_subset.groupby(['input_restaurant', 'input_address'])['stripped_domain'].nunique().reset_index()

# Return restaurants with fractured online presence
df_fractured = df_aggregated[df_aggregated['stripped_domain'] > 1]
df_fractured

# %%
