from bertopic import BERTopic
import pandas as pd
from umap import UMAP
import hdbscan
import re
import pickle
import numpy as np

#opening file with all the Twitter interactions
df_all = pd.read_csv('./bachelorproject/all.csv')

def filter(df):
    #seperating different types of tweets, return a list with dataframes of the different interactions
    own_tweets_df = df[df['referenced_tweets'].isna()].reset_index(drop=True)
    other_things_df = df[df['referenced_tweets'].notna()]

    retweets_df = other_things_df.loc[other_things_df['referenced_tweets'].str.contains("retweeted", case=False)].reset_index(drop=True)
    replies_df = other_things_df.loc[other_things_df['referenced_tweets'].str.contains("replied_to", case=False)].reset_index(drop=True)
    quoted_df = other_things_df.loc[other_things_df['referenced_tweets'].str.contains("quoted", case=False)].reset_index(drop=True)
    
    tweets_df = own_tweets_df.append(retweets_df, ignore_index=True)
    tweets_df = tweets_df.append(replies_df, ignore_index=True)
    tweets_df = tweets_df.append(quoted_df, ignore_index=True)
    df3 = own_tweets_df.append(other_things_df, ignore_index=True)
    
    return [own_tweets_df,retweets_df,replies_df,quoted_df, df3]

def remove_usernames_links(df):
    #removeing links and usernames
    df = re.sub('@[^\s]+','',df)
    df = re.sub('http[^\s]+','',df)
    return df

def preparing(df):
    #making timestamps for the over time visualisation. 
    dates = df
    dates['ConvertedDate']=df['created_at'].astype(str)
    dates['year'] = dates['ConvertedDate'].str[:4]
    dates = dates['year']
    timestamps = dates.to_list()
    
    #cleaning the dataframe
    df  = df['text']
    df = df.apply(remove_usernames_links)
    df = df.dropna()
    df = df.astype("string")
    
    
    return [df, timestamps]
    
def vis_topics(df,timestamps):

    #in this function the clustering and visualisation is performed
    corpus= df
    #loading the precomputed sentence embeddings. This way the program is much much faster. I would advise to save the embeddings in a npy file.
    #make sure that the embeddings have the same size as the dataframes, otherwise an error will be thrown.
    embeddings = np.load('own_tweet_embeddings.npy')
    #model for reducing the dimensions of the sentence embeddings
    umap_model = UMAP(n_neighbors=15, n_components=5, min_dist=0.0, metric='cosine')
    
    #for our reseacrh we used:
    #min_topic_size : 400 with own tweets
    #min_topic_size : 300 with retweets
    #min_topic_size : 600 for all

    #clustering model
    topic_model = BERTopic(umap_model = umap_model,hdbscan_model=None, n_gram_range = [1,1], calculate_probabilities= False, min_topic_size = 400, verbose=True )
    topics, probs = topic_model.fit_transform(documents=corpus, embeddings=embeddings)

    #saving with Tweet was assigned which topic for later. 
    tweet_topic_assignment = pd.DataFrame(topics)
    tweet_topic_assignment.to_csv('tweet_topics.csv', encoding='utf-8', index=False)
 
    #save the cluster info for each cluser number. Creates a file containing topic number, size and the most important words.
    info = topic_model.get_topic_info()
    info.to_csv('tweet_topic_descriptons.csv', encoding='utf-8', index=False)
    
    #show identified clusters in a 2d vector space
    topic_vis = topic_model.visualize_topics()
    topic_vis.show()

    #make similairity matrix for all clusters
    topic_heatmap = topic_model.visualize_heatmap()
    topic_heatmap.show()
    
    #generate the over time graphs, show the 5 largest clusters. 
    over_time = topic_model.topics_over_time(corpus, topics, timestamps)
    graph =topic_model.visualize_topics_over_time(over_time, topics=[0,1,2,3,4])
    graph.show()



list_of_df = filter(df_all)
data = preparing(list_of_df[0])
vis_topics(data[0],data[1])
