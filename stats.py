import pandas as pd
import numpy as np
import json
import csv
from scipy.stats import f
from scipy.stats import ttest_rel

df2 = pd.read_csv('./Visualizations2/retweets/retweets_general_topics.csv', delimiter=';')
df = pd.read_csv('./Visualizations2/tweets/tweets_general_topics.csv',delimiter=';')

def building_frame(df,df2):
    werk = df[["author_id"]]
    werk2 = df2[["author_id"]]
    df2 = pd.concat([df2.set_index('author_id'), werk.set_index('author_id')],axis=1)
    df = pd.concat([df.set_index('author_id'), werk2.set_index('author_id')],axis=1)
   
    df =df[["Corporate","Leadership","Politics","Other"]]
    df2 =df2[["Corporate","Leadership","Politics","Other"]] 
    combined = pd.concat([df,df2],axis=0,ignore_index=True)
    df = df.fillna(int(0))
    df2 = df2.fillna(int(0))
    df = df.astype({"Corporate" : "int","Leadership": "int","Politics": "int","Other": "int"})
    df2 = df2.astype({"Corporate" : "int","Leadership": "int","Politics": "int","Other": "int"})
    df2.loc['total'] = df2.sum(numeric_only=True, axis=0)
      
    return [df,df2]


def t_tests(df, df2):
    #perform a paired t test on each column between 
    Corporate = ttest_rel(df['Corporate'], df2['Corporate'])
    Leadership = ttest_rel(df['Leadership'], df2['Leadership'])
    Politics = ttest_rel(df['Politics'], df2['Politics'])
    Other = ttest_rel(df['Other'], df2['Other'])
    print("Corporate T-test: " + str(Corporate))
    print("Leadership T-test: " + str(Leadership))
    print("Politics T-test: " + str(Politics))
    print("Other T-test: " + str(Other))
    

data = building_frame(df, df2)
t_tests(data[0], data[1])
