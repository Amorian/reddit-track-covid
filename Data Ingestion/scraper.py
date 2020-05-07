# Make sure to install the reddit-scraper environment first and activate it, using Anaconda. The environment.yml file is provided for the easy install.

import os
import datetime as dt

from psaw import PushshiftAPI
import pandas as pd

import warnings

# Ignore PSAW warnings
warnings.filterwarnings('ignore')

def main():

    # Subreddits and folders
    subreddits = ['Coronavirus', 'COVID', 'COVID19', 'CoronavirusUS', 'nCoV', 'COVID19_support', 'China_Flu']
    folders = ['posts', 'comments']

    # Read data and store based on the date, and chunk into weeks to avoid memory overhead
    for sub in subreddits:
        for dir in folders:
            if not os.path.exists('../data/' + sub + '/' + dir):
                os.makedirs('../data/' + sub + '/' + dir)
        start_date = dt.datetime(2019, 12, 1)
        end_date = dt.datetime.now()
        while start_date < end_date:
            posts_download(sub, start_date)
            comments_download(sub, start_date)
            start_date += dt.timedelta(days=7)

# Download all posts for a week
def posts_download(subreddit, start_date):
    api = PushshiftAPI()

    # Timestamps
    end_date = start_date + dt.timedelta(days=7)
    start = int(start_date.timestamp())
    end = int(end_date.timestamp())
    df = pd.DataFrame()

    # Get all data in the correct format and save
    while True:
        gen = api.search_submissions(after=start, before=end, subreddit=subreddit, limit=500, sort="asc", sort_type="created_utc")
        new_df = pd.DataFrame([thing.d_ for thing in gen])
        if(new_df.empty or start > end):
            break
        df = df.append(new_df)
        start = df.created_utc.iat[-1]
    if not df.empty:
        df.created_utc = pd.to_datetime(df.created_utc, unit='s')
        df.retrieved_on = pd.to_datetime(df.retrieved_on, unit='s')
        df.created = pd.to_datetime(df.created, unit='s')

    # Write to CSV
    df.to_csv('../data/' + subreddit + '/posts/' + start_date.strftime('%Y-%m-%d') + '_' + end_date.strftime('%Y-%m-%d') + '.csv', index=False)
    print('r/' + subreddit + ' (Posts) : ' + start_date.strftime('%Y-%m-%d') + ' - ' + end_date.strftime('%Y-%m-%d'))

# Comments download
def comments_download(subreddit, start_date):
    api = PushshiftAPI()

    # Timestamps
    end_date = start_date + dt.timedelta(days=7)
    start = int(start_date.timestamp())
    end = int(end_date.timestamp())
    df = pd.DataFrame()

    # Get all data in the correct format and save
    while True:
        gen = api.search_comments(after=start, before=end, subreddit=subreddit, limit=500, sort="asc", sort_type="created_utc")
        new_df = pd.DataFrame([thing.d_ for thing in gen])
        if(new_df.empty or start > end):
            break
        df = df.append(new_df)
        start = df.created_utc.iat[-1]
    if not df.empty:
        df.created_utc = pd.to_datetime(df.created_utc, unit='s')
        df.retrieved_on = pd.to_datetime(df.retrieved_on, unit='s')
        df.created = pd.to_datetime(df.created, unit='s')

    # Write to CSV
    df.to_csv('../data/' + subreddit + '/comments/' + start_date.strftime('%Y-%m-%d') + '_' + end_date.strftime('%Y-%m-%d') + '.csv', index=False)
    print('r/' + subreddit + ' (Comments) : ' + start_date.strftime('%Y-%m-%d') + ' - ' + end_date.strftime('%Y-%m-%d'))

if __name__ == '__main__':
    main()
