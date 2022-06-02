import re
import pandas as pd
import numpy as np
import json
import csv
import time
from sentence_transformers import SentenceTransformer, util

#This file build the sentence embeddings. I would advise to precompute the embeddings and save them in a npy file as done here.
# 

df_all = pd.read_csv('./bachelorproject/all.csv')

def filter(df):
    #seperating different types of tweets
    own_tweets_df = df[df['referenced_tweets'].isna()].reset_index(drop=True)
    other_things_df = df[df['referenced_tweets'].notna()]

    retweets_df = other_things_df.loc[other_things_df['referenced_tweets'].str.contains("retweeted", case=False)].reset_index(drop=True)
    replies_df = other_things_df.loc[other_things_df['referenced_tweets'].str.contains("replied_to", case=False)].reset_index(drop=True)
    quoted_df = other_things_df.loc[other_things_df['referenced_tweets'].str.contains("quoted", case=False)].reset_index(drop=True)
    
    df3 = own_tweets_df.append(other_things_df, ignore_index=True)
    
    return [own_tweets_df, retweets_df,replies_df,quoted_df, df3]

def remove_usernames_links(df):
    df = re.sub('@[^\s]+','',df)
    df = re.sub('http[^\s]+','',df)
    return df

def preparing(df):
    df  = df['text']
    
    df = df.apply(remove_usernames_links)
    df = df.dropna()
    df = df.astype("string")
    
    return [df]

def embed(df):
    df = df.dropna()
    df = df.astype("string")
    df =df.reset_index(drop= True)
    # Model for computing embeddings
    model = SentenceTransformer('all-MiniLM-L6-v2')
    corpus= df.values.tolist()
    
    #the model requires a list filled with the Tweets.
    print("Encode the corpus. This might take a while")
    corpus_embeddings = model.encode(corpus, batch_size=64, show_progress_bar=True)
    embeddings = np.array(corpus_embeddings)
    #save the embeddings to a npy file. 
    np.save('retweet_embeddings.npy', embeddings)
   
    
list_of_df = filter(df_all)
data = preparing(list_of_df[1])
embed(data[0])
