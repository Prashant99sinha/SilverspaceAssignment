import pandas as pd
import streamlit as st
from datetime import datetime

# Ingest the data
def ingest_data(file_path):
    ##Ingest the data from the TSV file into a pandas DataFrame.
    chunks = pd.read_csv(file_path, sep='\t', low_memory=False, parse_dates=['ts1'], chunksize=10000)
    df = pd.concat(chunks, ignore_index=True)
    return df

# Query 1: Daily Term Frequency
def tweets_per_day(df, term):
    df['ts1'] = pd.to_datetime(df['ts1'], errors='coerce')
    term_tweets = df[df['text'].str.contains(term, case=False, na=False)]
    daily_count = term_tweets.groupby(term_tweets['ts1'].dt.date).size()
    return daily_count

# Query 2: Unique Users Posting the Term
def unique_user_count(df, term):
    term_tweets = df[df['text'].str.contains(term, case=False, na=False)]
    unique_users = term_tweets['author_id'].nunique()
    return unique_users

# Query 3: Average Likes for Tweets Containing the Term
def get_average_likes(df, term):
    df['like_count'] = pd.to_numeric(df['like_count'], errors='coerce')
    term_tweets = df[df['text'].str.contains(term, case=False, na=False)]
    average_likes = term_tweets['like_count'].mean()
    return average_likes

# Query 4: Locations (Place IDs) for Tweets
def get_place_ids(df, term):
    term_tweets = df[df['text'].str.contains(term, case=False, na=False)]
    place_ids = term_tweets['place_id'].dropna().unique()
    return place_ids

# Query 5: Tweet Posting Times
def get_tweet_times(df, term):
    term_tweets = df[df['text'].str.contains(term, case=False, na=False)].copy()
    term_tweets.loc[:, 'hour'] = term_tweets['ts1'].dt.hour
    hourly_distribution = term_tweets.groupby('hour').size()
    return hourly_distribution

# Query 6: Most Active User Posting the Term
def get_most_active_user(df, term):
    term_tweets = df[df['text'].str.contains(term, case=False, na=False)]
    most_active_user = term_tweets.groupby('author_id').size().idxmax()
    return most_active_user

# Streamlit app starts here
st.title('Twitter Data Analysis')

# Ingest data
default_file_path = 'correct_twitter_202102.tsv'
file_path = st.text_input('Enter the file path:', default_file_path)
if file_path:
    df = ingest_data(file_path)
    
    search_term = st.text_input('Enter the search term (e.g., "britney"):')
    
    if search_term:
        st.write("### Daily Tweet Frequency")
        st.write(tweets_per_day(df, search_term))
        
        st.write("### Unique Users Posting the Term")
        st.write(unique_user_count(df, search_term))
        
        st.write("### Average Likes for Tweets Containing the Term")
        st.write(get_average_likes(df, search_term))
        
        st.write("### Place IDs for Tweets Containing the Term")
        st.write(get_place_ids(df, search_term))
        
        st.write("### Tweet Posting Times (Hourly)")
        st.write(get_tweet_times(df, search_term))
        
        st.write("### Most Active User Posting the Term")
        st.write(get_most_active_user(df, search_term))
