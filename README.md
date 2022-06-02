# Bachelor-Thesis-CEO-Twitter-Communication

This repository contains the Python files I used for my thesis on CEO social media communication. To scrape Twitter I used a academic Twitter account. The calls that are made here cannot be made with a regular Twitter developer account. The order in how to call the files is:
1. scraping.py, if you do not have an academic account the searchtweets module will not work. You will need to change the entire file to make it work.
2. embedings.py, makes the precomputed sentence embeddings using sentence transformer
3. visualize.py, does the clustering and outputs figures
4. stats.py, I used this to perform paired T-tests. To actaully run this file a lot of extra work in excel is needed. Would not recommend.

I would advise to run the program in the Anaconda environment since a lot of modules did not work when I tried to PIP install them. I worked in a 3.9.12 enviroment.

