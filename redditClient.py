#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Columbia EECS E6893 Big Data Analytics

from datetime import datetime
import pandas as pd
import praw
# from tweepy import OAuthHandler
# from tweepy import Stream
# from tweepy.streaming import StreamListener
import socket
import json

# credentials
CLIENT_ID = ''
CLIENT_SECRET = ''
USERNAME = ''
PW = ''
USER_AGENT = 'testscript by u/{}'.format(USERNAME)

reddit = praw.Reddit(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    password=PW,
    user_agent=USER_AGENT,
    username=USERNAME,
)

# the tags to track
tags = ['jpm', 'jpmorgan', 'aapl', 'apple', 'pfe', 'pfizer', 'eth', 'ethereum', 'btc', 'bitcoin']
subs = 'news+worldnews+stocks+business+wallstreetbets+investing+stockmarket+crypto+cryptocurrency+bitcoin+etherium+dogecoin'

if __name__ == '__main__':
  try:
    # print(reddit.user.me())
    subreddit = reddit.subreddit(subs)
    comments = []
    for comment in subreddit.stream.comments():
      try:
          print(30*'_')
          print()
          parent_id = str(comment.parent())
          parent = reddit.comment(parent_id)
          submission = comment.submission
          # print(type(parent)) # comment
          # print('Parent:')
          # print(parent.body)
          # print('Reply')
          # print(comment.body)
          # print(' -- wait -- ')
          if any(tag in comment.body for tag in tags):
            print('oh my !')
            comment_time = datetime.utcfromtimestamp(int(comment.created_utc)).strftime('%Y-%m-%d %H:%M:%S')
            comment_data = [comment_time, comment.body, comment.score, parent.body, submission.title, submission.score, submission.num_comments]
            comments.append(comment_data)
      except praw.exceptions.PRAWException as e:
          pass
  except KeyboardInterrupt:
    print('Interrupted')

    df = pd.DataFrame(comments, columns= ['Time', 'Comment', 'Upvotes', 'Parent', 'Submission', 'Submission Upvotes', 'Submission Num. Comments'])
    df.to_csv('reddit_samples.csv', index=False)
    
    print('Complete')
