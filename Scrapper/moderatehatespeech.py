# This script is tho be used for the failed cases while accessing moderate hate speech API
import json
import os
import time
import psycopg2
from datetime import date, datetime, timedelta
from pprint import pprint
import sys
import requests
from dotenv import load_dotenv

load_dotenv()
db_settings = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
}


url = "https://api.moderatehatespeech.com/api/v1/moderate/"

# Define the request data as a dictionary
data = {"token": "1a9c69c1967f0e7606720d41707b3d76", "text": ""}

# Define the headers
headers = {"Content-Type": "application/json"}

# Make the POST request


def moderate_api(string):
    try:
        attempts = 0
        data["text"] = string
        while attempts <= 5:
            response = requests.post(url, json=data, headers=headers)
            if response.status_code == 200:
                if response.json()["response"] == "No text string passed":
                    return "NA", 0.0
                return response.json()["class"], response.json()["confidence"]
            time.sleep(2)
            attempts += 1
        print(f"Maximum attempts have passed")
        return "Apifailure", 0.0
    except Exception as e:
        print(f"An error occurred: {e}")
        return "Apifailure", 0.0


def update_posts(primary_key, title, selftext, conn=None):
    c = conn.cursor()
    title_class, title_confidence = moderate_api(title)
    # print(title_class)
    selftext_class, selftext_confidence = moderate_api(selftext)
    c.execute(
        "UPDATE reddit_posts SET title_class = %s, title_confidence= %s, selftext_class =%s, selftext_confidence= %s WHERE post_id =%s",
        (
            title_class,
            title_confidence,
            selftext_class,
            selftext_confidence,
            primary_key,
        ),
    )
    conn.commit()


def update_comments(primary_key, body, conn=None):
    c = conn.cursor()
    body_class, body_confidence = moderate_api(body)
    c.execute(
        "UPDATE reddit_comments SET body_class = %s, body_confidence= %s WHERE comment_id =%s",
        (body_class, body_confidence, primary_key),
    )
    conn.commit()


def moderate_posts():
    conn = psycopg2.connect(**db_settings)
    string = "politics"
    c = conn.cursor()
    try:
        # get inital count for job stats
        c.execute("SELECT * FROM reddit_posts WHERE subreddit=  %s", (string,))
        rows = c.fetchall()
        for row in rows:
            update_posts(row[0], row[1], row[7], conn)
    except KeyboardInterrupt:
        print("Exiting....")
    finally:
        conn.close()


def moderate_comments():
    conn = psycopg2.connect(**db_settings)
    c = conn.cursor()
    try:
        # get inital count for job stats
        c.execute("SELECT * FROM reddit_comments WHERE subreddit= %s;", ("politics",))
        rows = c.fetchall()
        for row in rows:
            update_comments(row[0], row[5], conn)
    except KeyboardInterrupt:
        print("Exiting....")
    finally:
        conn.close()


if __name__ == "__main__":
    moderate_posts()
    moderate_comments()
