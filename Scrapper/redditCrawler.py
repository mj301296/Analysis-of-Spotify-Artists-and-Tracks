import json
import os
import time
import psycopg2
from datetime import date, datetime, timedelta
from pprint import pprint
import sys

import requests
from dotenv import load_dotenv

# Load the environment variables from the .env file
load_dotenv()


db_settings = {
    "dbname": os.getenv('DB_NAME'),
    "user": os.getenv('DB_USER'),
}

# Reddit API credentials
client_ids = os.getenv('CLIENT_IDS')
client_secrets = os.getenv('CLIENT_SECRETS')
client_id_list = client_ids.split(',')
client_secret_list = client_secrets.split(',')
client_id = client_id_list[0]
client_secret = client_secret_list[0]
current_index=0
username = os.getenv('USER_NAME')
password = os.getenv('PASSWORD')

auth = requests.auth.HTTPBasicAuth(client_id, client_secret)
data = {
    'grant_type': 'password',
    'username': username,
    'password': password,
}

headers = {'User-Agent': 'MyAPi/0.0.1'}

# Send a POST request to the token endpoint
token_url = 'https://www.reddit.com/api/v1/access_token'
response = requests.post(token_url, auth=auth, data=data, headers=headers)
TOKEN = response.json()['access_token']
headers = {**headers, **{'Authorization': f"bearer {TOKEN}"}}
base_url = 'https://oauth.reddit.com'



#modereate hatespeech api
moderate_url = "https://api.moderatehatespeech.com/api/v1/moderate/"
# Define the request data as a dictionary
moderate_data = {
            "token": "1a9c69c1967f0e7606720d41707b3d76",
            "text": ""
        }

# Define the headers
moderate_headers = {
    "Content-Type": "application/json"
}

#Generate new token with next credentials incase API limit is hit
def generate_token():
    global current_index
    global headers
    current_index = (current_index + 1) % len(client_id_list)
    client_id = client_id_list[current_index]
    client_secret = client_secret_list[current_index]
    auth = requests.auth.HTTPBasicAuth(client_id, client_secret)
    data = {
        'grant_type': 'password',
        'username': username,
        'password': password,
    }

    headers = {'User-Agent': 'MyAPi/0.0.1'}
    token_url = 'https://www.reddit.com/api/v1/access_token'
    response = requests.post(token_url, auth=auth, data=data, headers=headers)
    TOKEN = response.json()['access_token']
    headers = {**headers, **{'Authorization': f"bearer {TOKEN}"}}


def create_table(conn):
    c = conn.cursor()
    c.execute('''
            CREATE TABLE IF NOT EXISTS reddit_posts(
                post_id TEXT PRIMARY KEY,
                title TEXT,
                score INTEGER,
                author TEXT,
                date REAL,
                url TEXT,
                upvote_ratio REAL,
                selftext TEXT,
                subreddit TEXT,
                title_class TEXT,
                title_confidence REAL,
                selftext_class TEXT,
                selftext_confidence REAL
            );
    ''')

    c.execute('''
            CREATE TABLE IF NOT EXISTS reddit_comments(
                comment_id TEXT PRIMARY KEY,
                post_id TEXT,
                score INTEGER,
                author TEXT,
                date REAL,
                body TEXT,
                subreddit TEXT,
                body_class TEXT,
                body_confidence REAL,
                FOREIGN KEY (post_id) REFERENCES reddit_posts(post_id)
            );
    ''')

    c.execute('''
            CREATE TABLE IF NOT EXISTS reddit_job_stats(
                id SERIAL PRIMARY KEY,
                job_start TIMESTAMP,
                job_end TIMESTAMP,
                subreddit TEXT,
                posts_added INTEGER,
                comments_added INTEGER   
            );
    ''')

    conn.commit()


def subreddits(subreddit, num_posts):
    conn = psycopg2.connect(**db_settings)
    create_table(conn)
    c = conn.cursor()
    # get inital count for job stats
    c.execute('SELECT COUNT(*) FROM reddit_posts;')
    initial_post_count = c.fetchone()[0]
    c.execute('SELECT COUNT(*) FROM reddit_comments;')
    initial_comment_count = c.fetchone()[0]
    conn.commit()
    job_start = datetime.now()
    try:
        # for subreddit in list:
        scrape_posts(subreddit, num_posts, conn)
    except KeyboardInterrupt:
        print('Exiting....')
    finally:
        update_stats(initial_post_count,
                     initial_comment_count, job_start, subreddit, conn)
        conn.close()


def moderate_api(string):
    try:
        attempts = 0
        moderate_data["text"] = string
        while(attempts <=5):
            response = requests.post(moderate_url, json=moderate_data, headers=moderate_headers)
            if response.status_code == 200:
                if(response.json()["response"] == 'No text string passed'):
                    return 'NA' , 0.0
                return response.json()["class"] , response.json()["confidence"]
            time.sleep(2)
            attempts += 1
        print(f"Maximum attempts have passed")
        return 'Apifailure' , 0.0
    except Exception as e:
        print(f"An error occurred: {e}")
        return 'Apifailure' , 0.0
    


def scrape_posts(subreddit_name, num_posts, conn=None):
    if subreddit_name == 'politics':
        endpoint = f'/r/{subreddit_name}/new?limit={num_posts}'
    else:
        endpoint = f'/r/{subreddit_name}/hot?limit={num_posts}'
    response = requests.get(base_url + endpoint, headers=headers)
    # print("scrape posts")
    try:
        response.raise_for_status()
        data = response.json()
        for post in data['data']['children']:
            insert_in_reddit_posts(post['data'], subreddit_name, conn)
            scrape_comments(post['data']['id'], subreddit_name, conn)
    except requests.exceptions.HTTPError as http_err:
        if response.status_code == 429:  # Rate limit hit
            print(f"Rate limit hit for client ID: {headers.get('Authorization')}")
            # Switch to the next client ID
            generate_token()
            # Retry the request
            scrape_posts(subreddit_name, num_posts, conn)
        else:
            print(f"HTTP error occurred: {http_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"Request error occurred: {req_err}")
    except json.JSONDecodeError as json_err:
        print(f"JSON decode error occurred: {json_err}")
    except Exception as e:
        print(f"An error occurred: {e}")


def scrape_comments(post_id, subreddit_name, conn=None):
    comments_url = f'{base_url}/r/{subreddit_name}/comments/{post_id}'
    comments_response = requests.get(comments_url, headers=headers)
    try:
        response.raise_for_status()
        comments_data = comments_response.json()
        for comment in comments_data[1]['data']['children']:
            insert_in_reddit_comments(
                comment['data'], subreddit_name, post_id, conn)
            iterate_replies(comment, subreddit_name, post_id, conn)
    except requests.exceptions.HTTPError as http_err:
        if response.status_code == 429:  # Rate limit hit
            print(f"Rate limit hit for client ID: {headers.get('Authorization')}")
            # Switch to the next client ID
            generate_token()
            # Retry the request
            scrape_comments(post_id,subreddit_name,conn)
        else:
            print(f"HTTP error occurred: {http_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"Request error occurred: {req_err}")
    except json.JSONDecodeError as json_err:
        print(f"JSON decode error occurred: {json_err}")
    except Exception as e:
        print(f"An error occurred: {e}")


def iterate_replies(comment, subreddit_name, post_id, conn=None):
    if 'replies' in comment['data'] and 'data' in comment['data']['replies']:
        for reply in comment['data']['replies']['data']['children']:
            try:
                insert_in_reddit_comments(
                comment['data'], subreddit_name, post_id, conn)
            except Exception as e:
                 print(f"An error occurred: {e}")
            iterate_replies(reply, subreddit_name, post_id, conn)

def insert_in_reddit_posts(data, subreddit_name, conn=None):
    c = conn.cursor()
    post_id = data['id']
    title = data['title']
    score = data['score']
    author = data['author']
    date = data['created_utc']
    url = data.get('url_overriden_by_dest')
    upvote_ratio = data['upvote_ratio']
    selftext = data['selftext']
    title_class, title_confidence = moderate_api(title)
    selftext_class, selftext_confidence = moderate_api(selftext)
    c.execute('INSERT INTO reddit_posts(post_id, title, score, author, date, url, upvote_ratio, selftext, subreddit,  title_class, title_confidence, selftext_class, selftext_confidence) '
              'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) '
              'ON CONFLICT (post_id) DO NOTHING',
              (post_id, title, score, author, date, url, upvote_ratio, selftext, subreddit_name, title_class, title_confidence, selftext_class, selftext_confidence))
    conn.commit()


def insert_in_reddit_comments(data, subreddit_name, post_id, conn=None):
    c = conn.cursor()
    comment_id = data['id']
    body = data['body']
    score = data['score']
    author = data['author']
    date = data['created_utc']
    body_class, body_confidence = moderate_api(body)
    c.execute('INSERT INTO reddit_comments(comment_id, post_id, score, author, date, body, subreddit, body_class, body_confidence) '
              'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) '
              'ON CONFLICT (comment_id) DO NOTHING',
              (comment_id, post_id, score, author, date, body, subreddit_name, body_class, body_confidence))
    conn.commit()


def update_stats(initial_post_count, initial_comment_count, job_start, subreddit, conn=None):
    c = conn.cursor()
    c.execute('SELECT COUNT(*) FROM reddit_posts;')
    final_post_count = c.fetchone()[0]
    c.execute('SELECT COUNT(*) FROM reddit_comments;')
    final_comment_count = c.fetchone()[0]
    job_end = datetime.now()
    posts_added = final_post_count - initial_post_count
    comments_added = final_comment_count - initial_comment_count
    c.execute('INSERT INTO reddit_job_stats(job_start, job_end, subreddit ,posts_added, comments_added) '
              'VALUES (%s, %s, %s, %s, %s)',
              (job_start, job_end, subreddit, posts_added, comments_added))
    conn.commit()


if __name__ == "__main__":
    num_args = len(sys.argv) - 1
    # Access individual command-line arguments
    if num_args >= 1:
        subreddit = sys.argv[1]  # The first argument
    print(f"subreddit: {subreddit}")
    print("Running reddit scrapper....")
    num_posts_to_scrape = 50  # Adjust as needed
    subreddits(subreddit, num_posts_to_scrape)




