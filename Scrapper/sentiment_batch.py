# This script is tho be used for the failed cases while accessing moderate hate speech API
import json
import os
import time
import psycopg2
from datetime import date, datetime, timedelta
from pprint import pprint
import sys
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

import requests
from dotenv import load_dotenv

sid = SentimentIntensityAnalyzer()
nltk.download("vader_lexicon")


load_dotenv()
db_settings = {
    "dbname": "scrappeddata",
    "user": "postgres",
}


# Make the POST request


def create_table(conn):
    c = conn.cursor()
    c.execute(
        """
            CREATE TABLE IF NOT EXISTS sentiment_posts(
                post_id TEXT PRIMARY KEY,
                title_neg REAL,
                title_neu REAL,
                title_pos REAL,
                title_compound REAL,
                selftext_neg REAL,
                selftext_neu REAL,
                selftext_pos REAL,
                selftext_compound REAL
            );
    """
    )

    c.execute(
        """
            CREATE TABLE IF NOT EXISTS sentiment_comments(
                comment_id TEXT PRIMARY KEY,
                body_neg REAL,
                body_neu REAL,
                body_pos REAL,
                body_compound REAL
            );
    """
    )

    conn.commit()


def sentiment(text):
    ss = sid.polarity_scores(text)
    return ss


def update_posts(primary_key, title, selftext, conn=None):
    c = conn.cursor()
    title_score = sentiment(title)
    selftext_score = sentiment(selftext)

    c.execute(
        "INSERT INTO sentiment_posts(post_id, title_neg, title_neu, title_pos, title_compound, selftext_neg, selftext_neu, selftext_pos, selftext_compound) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
        "ON CONFLICT (post_id) DO UPDATE "
        "SET title_neg = EXCLUDED.title_neg, "
        "    title_neu = EXCLUDED.title_neu, "
        "    title_pos = EXCLUDED.title_pos, "
        "    title_compound = EXCLUDED.title_compound, "
        "    selftext_neg = EXCLUDED.selftext_neg, "
        "    selftext_neu = EXCLUDED.selftext_neu, "
        "    selftext_pos = EXCLUDED.selftext_pos, "
        "    selftext_compound = EXCLUDED.selftext_compound",
        (
            primary_key,
            title_score["neg"],
            title_score["neu"],
            title_score["pos"],
            title_score["compound"],
            selftext_score["neg"],
            selftext_score["neu"],
            selftext_score["pos"],
            selftext_score["compound"],
        ),
    )

    conn.commit()


#    reddit_post_sentiment
# title:  {'neg': 0.0, 'neu': 1.0, 'pos': 0.0, 'compound': 0.0}
# selftext: {'neg': 0.0, 'neu': 1.0, 'pos': 0.0, 'compound': 0.0}

# reddit_comment_sentiment
# body: {'neg': 0.0, 'neu': 1.0, 'pos': 0.0, 'compound': 0.0}


def update_comments(primary_key, body, conn=None):
    c = conn.cursor()
    body_score = sentiment(body)

    c.execute(
        "INSERT INTO sentiment_comments(comment_id, body_neg, body_neu, body_pos, body_compound) "
        "VALUES (%s, %s, %s, %s, %s) "
        "ON CONFLICT (comment_id) DO UPDATE "
        "SET body_neg = EXCLUDED.body_neg, "
        "    body_neu = EXCLUDED.body_neu, "
        "    body_pos = EXCLUDED.body_pos, "
        "    body_compound = EXCLUDED.body_compound ",
        (
            primary_key,
            body_score["neg"],
            body_score["neu"],
            body_score["pos"],
            body_score["compound"],
        ),
    )

    conn.commit()


def moderate_posts():
    conn = psycopg2.connect(**db_settings)
    create_table(conn)
    string = "Apifailure"
    c = conn.cursor()
    try:
        # get inital count for job stats
        c.execute("SELECT * FROM reddit_posts;")
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
        c.execute("SELECT * FROM reddit_comments;")
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
