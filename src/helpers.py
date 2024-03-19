# helpers.py

import pandas as pd
import difflib
import logging


def remove_blacklisted_domains(results_data, link_column='link'):

    blacklist = [
        "mapquest", 
        "yelp", "restaurantji", "restaurantguru",
        "propertyshark", "loopnet",
        "tripadvisor", "roadtrippers",
        "slicelife", "grubhub", "doordash",
        "instagram", "facebook",
        "toasttab", "fromtherestaurant", "autoreserve",
        "opentable", "foursquare", "linkedin.com",
        ".business",
        ".square",
        "seamless.com",
        "ezcater.com",
        "yellowpages.com", "menupages.com",
        "ubereats.com", "beyondmenu.com"
    ]
    
    def contains_keyword(link):
        return any(keyword in link for keyword in blacklist)
    
    filtered_df = results_data[~results_data[link_column].apply(contains_keyword)]
    
    return filtered_df


def remove_correlated_domains(results_data, link_column):

    results_data_copy = results_data.copy()

    def strip_domain(link):
        stripped_protocol = link.split("//")[-1]                # Step 1: Strip protocol and anything before "//"
        stripped_www = stripped_protocol.replace("www.", "")    # Step 2: Strip "www."
        domain_only = stripped_www.split("/")[0]                # Step 3: Keep everything before the first "/"
        final_domain = ".".join(domain_only.split(".")[:2])     # Step 4: Keep the part before the first period and the subsequent string
        return final_domain
    
    results_data_copy.loc[:, 'stripped_domain'] = results_data_copy[link_column].apply(strip_domain)
    unique_domains = set(results_data_copy['stripped_domain'])
    stripped_df = results_data_copy[results_data_copy['stripped_domain'].isin(unique_domains)]
    
    return stripped_df


def remove_location_domains(results_data):
    
    def clean_token(token):
        """Removes non-alphanumeric characters, converts to lowercase, and excludes single-letter tokens."""
        cleaned_token = ''.join(char for char in token if char.isalnum()).lower()
        return cleaned_token if len(cleaned_token) > 1 else ''

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
        
        # print("Restaurant: ", restaurant_tokens)
        # print("Address: ", address_tokens)
        # print("Domain: ", domain)
        # print("Refined Restaurant: ", refined_restaurant_tokens)

        if all_address_tokens_in_domain(address_tokens, domain) and no_refined_restaurant_tokens_in_domain(refined_restaurant_tokens, domain):
            continue
        else:
            indices_to_keep.append(index)

    return results_data.loc[indices_to_keep]


def remove_low_similarity_domains(results_data):

    def calculate_similarity_scores(domain_list):
        scores = []
        for i in range(len(domain_list)):
            for j in range(i+1, len(domain_list)):
                score = difflib.SequenceMatcher(None, domain_list[i], domain_list[j]).ratio()
                scores.append(score)
        return scores

    def has_high_similarity(scores, threshold=0.7):
        return any(score > threshold for score in scores)

    if not results_data.empty:

        results_data['similarity_scores'] = results_data['unique_domain_list'].apply(calculate_similarity_scores)
        
        results_data = results_data[results_data['similarity_scores'].apply(has_high_similarity)]
    
    if 'similarity_scores' in results_data.columns:
        results_data = results_data.drop(columns=['similarity_scores'])
    
    return results_data


def standardize_columns(input_data):
    new_columns = input_data.columns
    new_columns = new_columns.str.lower()
    new_columns = new_columns.str.replace(r'\W+', '_', regex=True)
    new_columns = new_columns.str.strip('_')
    input_data.columns = new_columns
    return input_data


def process_row(row, scraper_api):

    logging.info(f"Processing row for {row['account_name']} at {row['billing_address_line_1']}")
    full_query = f"{row['account_name']} {row['billing_address_line_1']}"
    df_results = scraper_api.get_search_results(query_restaurant=full_query, query_location=row['billing_city'])
    
    df_reduced = remove_blacklisted_domains(df_results, 'link')
    logging.info(f"Reduced domains: {len(df_reduced)} from initial results")
    df_stripped = remove_correlated_domains(df_reduced, 'link')
    df_subset = remove_location_domains(df_stripped)
    
    df_aggregated = df_subset.groupby(['input_restaurant', 'input_city'])['stripped_domain'].agg(lambda x: list(set(x))).reset_index()
    df_aggregated.rename(columns={'stripped_domain': 'unique_domain_list'}, inplace=True)
    
    if not df_aggregated.empty:
        df_aggregated['unique_domain_count'] = df_aggregated['unique_domain_list'].apply(len)
        df_fractured = df_aggregated[df_aggregated['unique_domain_count'] > 1]
        df_fractured_with_similarity = remove_low_similarity_domains(df_fractured)
        logging.info(f"Found {len(df_fractured_with_similarity)} fractured domains with high similarity scores")
        return df_fractured_with_similarity
    else:
        logging.info("No fractured domains found or similarity scores below threshold")
        return pd.DataFrame()
        


def batch_process(filepath, scraper_api, sample_n):
    logging.info(f"Starting batch process for {sample_n} restaurants")
    df_raw = pd.read_csv(filepath)
    logging.info("CSV loaded successfully")
    df_formatted = standardize_columns(df_raw)
    df_sampled = df_formatted.sample(n=int(sample_n), random_state=42)
    results_list = []

    for index, row in df_sampled.iterrows():
        result = process_row(row, scraper_api)
        if not result.empty:
            results_list.append(result)

    final_results = pd.concat(results_list, ignore_index=True) if results_list else pd.DataFrame()
    return final_results
