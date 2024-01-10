import subprocess
import time
from datetime import datetime
import os
from dotenv import load_dotenv
# Load the environment variables from the .env file
load_dotenv()

subreddit_list = os.getenv('SUBREDDITS')
subreddits = subreddit_list.split(',')
current_index =0
# print(subreddits)


# Function to execute the Spotify script
def run_spotify_script():
    subprocess.call(['python3', 'spotifyCrawler.py'])  

# Function to execute the Reddit script
def run_reddit_script(subreddit):
    subprocess.call(['python3', 'redditCrawler.py', subreddit])  

# Main loop
spotify_interval = 10 * 60 * 60  
reddit_interval = 5 * 60  

run_spotify_script()
spotify_last_executed = time.time()
run_reddit_script(subreddits[current_index])
current_index = (current_index + 1) % len(subreddits)
reddit_last_executed = time.time()

while True:
    current_time = time.time()
    # print(current_time - spotify_last_executed)

    if current_time - spotify_last_executed >= spotify_interval:
        run_spotify_script()
        spotify_last_executed = current_time
    # print(current_time - reddit_last_executed)
    if current_time - reddit_last_executed >= reddit_interval:
        run_reddit_script(subreddits[current_index])
        current_index = (current_index + 1) % len(subreddits)
        reddit_last_executed = current_time

    time.sleep(60)  # Check every 60 seconds
