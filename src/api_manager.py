# api_manager.py

from serpapi import GoogleSearch
import pandas as pd


class SerpAPI():    

    def __init__(self, api_key):
        
        self.api_key = api_key


    def extract_organic_results(self,
                                response_dict):

        organic_results = response_dict.get("organic_results", [])
        if not organic_results:
            return []

        results_data = []
        keys_to_extract = organic_results[0].keys()
        
        for result in organic_results:
            result_data = {key: result.get(key, None) for key in keys_to_extract}
            results_data.append(result_data)
        
        return results_data


    def get_search_results(self,
                           query_restaurant, 
                           query_location=None):
        
        # Set payload
        params = {
            "q": query_restaurant,
            "hl": "en",
            "gl": "us",
            "google_domain": "google.com",
            "api_key": self.api_key
        }    
        if query_location:
            params["location"] = query_location

        # Execute search
        response_raw = GoogleSearch(params)
        response_dict = response_raw.get_dict()
        
        # Parse nested json
        transformed_dict = self.extract_organic_results(response_dict)
        
        # Format dataframe
        results_df = pd.DataFrame(transformed_dict)
        results_df['input_restaurant'] = query_restaurant
        results_df['input_city'] = query_location
        cols_search_results = ['input_restaurant', 'input_city', 'position', 'title', 
                            'link', 'displayed_link']
        optional_cols = ['snippet', 'snippet_highlighted_words']

        final_cols = cols_search_results + [col for col in optional_cols if col in results_df.columns]

        return results_df[final_cols]
    