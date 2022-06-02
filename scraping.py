from searchtweets import load_credentials, collect_results
import requests
import os
import pandas as pd
import gspread
import json
import time
import numpy as np
from oauth2client.service_account import ServiceAccountCredentials
from df2gspread import df2gspread as d2g

#Much of the code is written by Miros Zohrevand, I just made some changes.
class Scrape():

    def __init__(self,mode1,mode2):
        #twitter creds
        self.creds_arg = load_credentials(filename= "<insert the path to your yaml file>",yaml_key="search_tweets_v2",
                             env_overwrite=False)
        self.bearer_token = self.creds_arg["bearer_token"]

        #google drive creds
        self.scope = ['https://www.googleapis.com/auth/drive']
        self.creds = ServiceAccountCredentials.from_json_keyfile_name('<insert own credential json>', self.scope)
        
        if mode1:
            #calling functions to get account data
            print("mode1 is activated, a sheet containing CEO twitter account information will be made.")
            df= self.ceo_dataframe()
            self.output(df)
        if mode2:
            #calling functions to scrape tweets
            print("mode2 is activated, a sheet containing CEO tweets will be made.")
            input_df =self.ceo_id()
            self.get_tweets(input_df)

        if not mode1 and not mode2:
            print("Both modes are disabled, maybe change the modes to be true?")

    def create_url(self,user_list = ["MirosZohrehvand"]):
        #create custom endpoint url for the API
        user_ids = user_list[0] if len(user_list) == 1 else ",".join(user_list)
        return "https://api.twitter.com/2/users/by?usernames={}".format(user_ids)

    def get_params(self):
        # User fields are adjustable.
        # Options include:
        # created_at,description,entities,id,location,
        # name,pinned_tweet_id,profile_image_url,protected,public_metrics,url,username,verified,withheld
        return {"user.fields" : "created_at,description,id,location,name,pinned_tweet_id,profile_image_url,protected,public_metrics,url,username,verified,withheld"}

    def bearer_oauth(self,r):
        """
        Method required by bearer token authentication.
        """
        #ask Miros about this
        r.headers["Authorization"] = f"Bearer {self.bearer_token}"
        r.headers["User-Agent"] = "v2UserTweetsPython"
        return r


    def connect_to_endpoint(self,url, params):
        #connecting to endpoint, make the request
        response = requests.request("GET", url, auth=self.bearer_oauth, params=params)
        print(response.status_code)
        if response.status_code != 200:
            raise Exception(
                "Request returned an error: {} {}".format(
                    response.status_code, response.text
                )
            )
        return response.json()

    def ceo_dataframe(self):
        # use creds to create a client to interact with the Google Drive API
        # here the Twitter handles are pulled from a google sheet, but you could use any method you want.
        client = gspread.authorize(self.creds)
        worksheet = client.open("<insert own sheet name").sheet1

        # Extract all values and perform some operations to get the desired dataframe
        rows = worksheet.get_all_values()
        ceo_df = pd.DataFrame.from_records(rows[1:], columns=rows[0])
        ceo_df["twitter_handle_clean"] = [np.nan if (elem == "na") or (elem == "") or ("Duplicate" in elem) else elem.strip() for elem in ceo_df["twitter_handle_clean"] ]
        ceo_df["twitter_handle_clean"] = ceo_df["twitter_handle_clean"].str.lower()
        ceo_df = ceo_df.dropna()
        ceo_df = ceo_df.reset_index()
        ceo_df= ceo_df[['twitter_handle_clean','exec_fullname','coname']]
        return ceo_df

    def output(self,ceo_df):
        api_output = []
        #loop through df, make steps of 95 rows.
        for i in range(0, len(ceo_df), 95):
            user_list = ceo_df.twitter_handle_clean[i:i+95].tolist()
            url = self.create_url(user_list)
            params = self.get_params()
            json_response = self.connect_to_endpoint(url, params)
            users_df = pd.DataFrame.from_dict(json_response["data"])
            users_df.username = users_df.username.str.lower()
            api_output.append(users_df)

        api_output = pd.concat(api_output).reset_index(drop=True)
        api_output.drop_duplicates(["username"], inplace=True)
        input_df = ceo_df.merge(api_output, how="left", left_on="twitter_handle_clean", right_on="username")
        
        #upload results to google sheet, can also be a csv file or something else
        spreadsheet_key = '<insert own sheet key>'
        wks_name = 'Account_data'
        d2g.upload(input_df, spreadsheet_key, wks_name, credentials=self.creds, row_names=True)

    def ceo_id(self):
        #we open the previously made twitter account sheet from mode 1. Again if you used another method this needs to be changed.
        client = gspread.authorize(self.creds)
        worksheet = client.open("Account_data").sheet1
        rows = worksheet.get_all_values()
        ceo_df = pd.DataFrame.from_records(rows[1:], columns=rows[0])
        ceo_df= ceo_df[['twitter_handle_clean','exec_fullname','coname','id']]
        ceo_df = ceo_df.head(50)
        return ceo_df

    def get_tweets(self,df):
        query_params = {"tweet.fields": "id,author_id,created_at,text,referenced_tweets,public_metrics,lang,entities,conversation_id,in_reply_to_user_id,geo,possibly_sensitive,context_annotations,attachments,withheld,source",
                "max_results": "100"}
        query = json.dumps(query_params)

        df_list = []
        error_rows = []

        for index, row in df.iterrows():
            try:
                user_id = row['id']
                # set endpoint for each row
                self.creds_arg["endpoint"] = "https://api.twitter.com/2/users/{id}/tweets".format(id = user_id)

                tweets = collect_results(query,max_tweets=100000, result_stream_args=self.creds_arg)
                tweet_df = pd.concat([pd.DataFrame.from_dict(elem.get('data')) for elem in tweets])
                
                # merge tweets
                df_list.append(tweet_df)
                all_tweets_df = pd.concat(df_list)
                df_list = [all_tweets_df]

            except Exception as e:
                error_rows.append(index)
                print("Error for row: ", index,"\n")
                print(e)
                continue
            time.sleep(2)

        #adding total tweet count for each userid
        count_measure = pd.DataFrame(df_list[0].drop_duplicates('id').groupby('author_id')['id'].count())
        count_measure.reset_index(inplace=True)
        count_measure.columns = ["author_id","retrieved_count"]
        tweets = df_list[0].merge(count_measure, how='left')

        spreadsheet_key = '<insert you own key>'
        wks_name = 'Tweets'

        #uploading data frame to google sheets, ceo's who did not tweet results in an error and will not be added to the sheet
        d2g.upload(tweets, spreadsheet_key, wks_name, credentials=self.creds, row_names=True)
    
if __name__ == "__main__":
    mode1=True  #if True, account information for all CEOs will be scraped and added to a sheet
    mode2=False  #if True, all tweets of the CEOs are scraped. This requires mode 1 to have run once. 
    Scrape(mode1,mode2)
