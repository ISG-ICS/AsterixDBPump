# AsterixDB Pump

### Description:
This tool retrieves data from a source AsterixDB, and ingest into a destination AsterixDB.
For now it is builtin supporting twitter.ds_tweet. 

### Assumptions:
You have already have twitter.ds_tweet setup on both source and destination AsterixDB. Also TweetFeed should be enabled on the destination side.

### Requirements:
 - python@3.6+

### Install dependencies:
`pip install -r requirements.txt`


### Config
modify `config/pump_conf.ini`:
- change source and destination db ip
- change your start_time and end_time
- indicate your running mode: incremental or one-time
- specify your sql

### Run:
`python3 asterixdb_pump.py`
