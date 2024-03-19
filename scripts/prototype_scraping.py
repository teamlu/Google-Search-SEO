# %% 
# SETUP

# Libraries
import os
from serpapi import GoogleSearch
import pandas as pd
import difflib

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
    results_df['input_city'] = query_location
    cols_of_interest = ['input_restaurant', 'input_city', 'position', 'title', 'snippet', 'snippet_highlighted_words', 'link', 'displayed_link']
    
    return results_df[cols_of_interest]


# Define helper functions


def remove_blacklisted_domains(results_data, link_column='link'):

    blacklist = [
        "mapquest", 
        "yelp", "restaurantji", "restaurantguru",
        "propertyshark", "loopnet",
        "tripadvisor", "roadtrippers",
        "slicelife", "grubhub", "doordash",
        "instagram", "facebook",
        "toasttab", "fromtherestaurant", "autoreserve",
        "opentable", "foursquare", "linkedin.com"
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
        address_tokens = {clean_token(token) for token in row['input_city'].lower().split()}
        domain = row['stripped_domain'].lower()

        # Remove address tokens from restaurant tokens
        refined_restaurant_tokens = restaurant_tokens - address_tokens
        if '' in refined_restaurant_tokens:
            refined_restaurant_tokens.remove('')
        
        print("Restaurant: ", restaurant_tokens)
        print("Address: ", address_tokens)
        print("Domain: ", domain)
        print("Refined Restaurant: ", refined_restaurant_tokens)

        if all_address_tokens_in_domain(address_tokens, domain) and no_refined_restaurant_tokens_in_domain(refined_restaurant_tokens, domain):
            continue
        else:
            indices_to_keep.append(index)

    return results_data.loc[indices_to_keep]


def remove_low_similarity_domains(df):

    def calculate_similarity_scores(row):
        domain_list = row['unique_domain_list']
        scores = []
        for i in range(len(domain_list)):
            for j in range(i+1, len(domain_list)):
                score = difflib.SequenceMatcher(None, domain_list[i], domain_list[j]).ratio()
                scores.append(score)
        return scores

    def has_high_similarity(scores_list):
        return any(score > 0.7 for score in scores_list)
    
    df['similarity_scores'] = df.apply(calculate_similarity_scores, axis=1)
    filtered_df = df[df['similarity_scores'].apply(has_high_similarity)]
    filtered_df = filtered_df.drop(columns=['similarity_scores'])
    
    return filtered_df

# %%
# EXECUTION

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

# Test 4
q = "The Tavern Bar & Grill"
address = "75 East Colonial Drive"
city = "Orlando"

# Test 5
q = "Albertano's Authentic Mexican Food"
address = "1417 N 7Th Ave"
city = "Bozeman"


# Create search query, combining restaurant name and address
full_query = q + ' ' + address

# Input selection
df_results = get_search_results(query_restaurant=full_query, query_location=city)

# Link refinement
df_reduced = remove_blacklisted_domains(df_results, 'link')
df_stripped = remove_correlated_domains(df_reduced, 'link')
df_subset = remove_location_domains(df_stripped)

# Domain aggregation
df_aggregated = df_subset.groupby(['input_restaurant', 'input_city'])['stripped_domain'].agg(lambda x: list(set(x))).reset_index()
df_aggregated.rename(columns={'stripped_domain': 'unique_domain_list'}, inplace=True)

# Identify restaurants with fractured online presence
df_fractured = df_aggregated[df_aggregated['unique_domain_list'].apply(lambda x: len(x) > 1)]
df_fractured_with_similarity = remove_low_similarity_domains(df_fractured)
df_fractured_with_similarity['unique_domain_count'] = df_aggregated['unique_domain_list'].apply(len)
df_fractured_with_similarity

# %%
# SANDBOX

def clean_token(token):
    cleaned_token = ''.join(char for char in token if char.isalnum()).lower()
    return cleaned_token if len(cleaned_token) > 1 else ''

def token_in_domain(token, domain_tokens):
    cleaned_token = clean_token(token)
    return any(cleaned_token in clean_token(domain_token) for domain_token in domain_tokens)

def all_address_tokens_in_domain(address_tokens, domain_tokens):
    return all(token_in_domain(token, domain_tokens) for token in address_tokens)

def no_refined_restaurant_tokens_in_domain(refined_restaurant_tokens, domain_tokens):
    # Ensure each token is individually cleaned and compared against the domain tokens
    for token in refined_restaurant_tokens:
        cleaned_token = clean_token(token)  # Clean each token
        if cleaned_token and any(cleaned_token in clean_token(domain_token) for domain_token in domain_tokens):
            print(f"Match found: {cleaned_token} in domain")
            return False  # If any refined restaurant token is found in domain tokens, return False
    return True  # If no refined restaurant tokens are found in domain tokens, return True

# Now, when generating sets of tokens, make sure to exclude empty strings
restaurant_tokens = {'food', 'albertanos', 'ave', 'n', 'authentic', 'mexican', '7th', '1417'}
address_tokens = {'bozeman'}
domain_tokens = {'bozemanmagazine'}

# Remove empty strings and 'com' from domain tokens, if present
cleaned_domain_tokens = {clean_token(token) for token in domain_tokens if token and token != "com"}
cleaned_refined_restaurant_tokens = {clean_token(token) for token in restaurant_tokens - address_tokens if token}

# Perform checks
address_in_domain = all_address_tokens_in_domain(address_tokens, cleaned_domain_tokens)
refined_tokens_not_in_domain = no_refined_restaurant_tokens_in_domain(cleaned_refined_restaurant_tokens, cleaned_domain_tokens)

(address_in_domain, refined_tokens_not_in_domain)
# %%
