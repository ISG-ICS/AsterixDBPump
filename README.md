# AsterixDB Pump

### Description:
This tool retrieves data from a source AsterixDB, and ingest into a destination AsterixDB.
For now it is builtin supporting twitter.ds_tweet. 

### Assumptions:
You have already have twitter.ds_tweet and TweetFeed setup on both source and destination AsterixDB.

### Requirements:
 - python@3.6+

### Install dependencies:
`pip install -r requirements.txt`

### Run:
`python3 tweet_pump.py`