#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Columbia EECS E6893 Big Data Analytics

import datetime
import pandas as pd
from psaw import PushshiftAPI

api = PushshiftAPI()

# the tags to track
tags = ['jpm', 'jpmorgan', 'aapl', 'apple', 'pfe', 'pfizer', 'eth', 'ethereum', 'btc', 'bitcoin']
subs = ['news', 'worldnews', 'stocks', 'business', 'wallstreetbets', 'investing', 'stockmarket', 'crypto', 'cryptocurrency', 'bitcoin', 'etherium', 'dogecoin']

start_date = datetime.date(2019, 11, 1)
end_date = datetime.date(2021, 11, 30)

delta = end_date - start_date

if __name__ == '__main__':
  try:
    comments = []
    for i in range(delta.days):
      first_datetime = datetime.datetime.combine(start_date + datetime.timedelta(days=i), datetime.datetime.min.time())
      second_datetime = datetime.datetime.combine(start_date + datetime.timedelta(days=i+1), datetime.datetime.min.time())

      print('Dates: {0} to {1}'.format(first_datetime, second_datetime))

      for sub in subs:
        query = api.search_submissions(subreddit=sub, after=first_datetime, before=second_datetime, limit=500)
        for comment in query:
          if any(tag in comment.d_['title'] for tag in tags):
            comment_time = datetime.datetime.utcfromtimestamp(int(comment.created_utc)).strftime('%Y-%m-%d %H:%M:%S')
            comment_data = [comment_time, comment.d_['title'], comment.d_['score'], comment.d_['num_comments']]
            comments.append(comment_data)

      print(len(comments))

    df = pd.DataFrame(comments, columns= ['Time', 'Comment', 'Score', 'Num Comments'])
    df.to_csv('new_reddit_past_samples_try.csv', index=False)
    
  except KeyboardInterrupt:
    print('Interrupted')

    df = pd.DataFrame(comments, columns= ['Time', 'Comment', 'Score', 'Num Comments'])
    df.to_csv('new_reddit_past_samples_try.csv', index=False)
    
    print('Complete')
