# airlfow
airflow_twitter
import tweepy
import pandas as pd 
import json
from ntscraper import Nitter
from datetime import datetime
import s3fs 
scraper = Nitter(0)
def run_twitter_etl():

    consumer_key = "Sit16IHAM7nagkBFiHr1ByGVU" 
    consumer_secret = "2ZPTVsZRiqAEXpI4cEOrJN7MtKySkvimdFyvLlV4EdUlZSCmNY" 
    access_key = "3221526313-mEvKfYdIxPADE2vG84vEIn6aGBZkABIQnU5ApGN"
    access_secret = "OMlIbcPFRCXDaWQLqSBAtiUqIk5Da1WN5fKMyBepPoDy3"
    tweets = scraper.get_tweets("india", mode = 'hashtag', number=100)
    

    # auth = tweepy.OAuth1UserHandler(
    # consumer_key, consumer_secret,
    # access_key, access_secret
    # )

    # #Instantiate the tweepy API
    # api = tweepy.API(auth, wait_on_rate_limit=True)


    # search_query = "'ref''world cup'-filter:retweets AND -filter:replies AND -filter:links"
    # no_of_tweets = 100

    
    # #The number of tweets we want to retrieved from the search
    # tweets = api.search_tweets(q=search_query, lang="en", count=no_of_tweets, tweet_mode ='extended')
    
    # #Pulling Some attributes from the tweet
    # attributes_container = [[tweet.user.name, tweet.created_at, tweet.favorite_count, tweet.source, tweet.full_text] for tweet in tweets]

    # #Creation of column list to rename the columns in the dataframe
    # columns = ["User", "Date Created", "Number of Likes", "Source of Tweet", "Tweet"]
    
    # #Creation of Dataframe
    # tweets_df = pd.DataFrame(attributes_container, columns=columns)
    final_tweets = []
    for x in tweets['tweets']:
        data = [x['link'], x['text'],x['date'],x['stats']['likes'],x['stats']['comments']]
        final_tweets.append(data)
    tweets_df = pd.DataFrame(final_tweets, columns =['twitter_link','text','date','likes','comments'])
    tweets_df.to_csv('refined_tweets.csv')
    
    
print(run_twitter_etl())
