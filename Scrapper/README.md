# project-1-implementation-informatics

# CS515 - Social Media Data Science Pipeline - Project 1

## Table of Contents

- [Introduction](#introduction)
- [Author](#author)
- [Programming Language](#programming-language)
- [Execution Instructions](#execution-instructions)
- [Code Overview](#code-overview)
- [Additional Function](#additional-function)

## Introduction

This repository contains a Python program for data collection and
analysis. The program collects data from Reddit using the Reddit API and
Spotipy API. 

## Author

- **Name**: Akshat Shah
- **Email**: ashah85@binghamton.edu
- **B-Number**: B00969887

- **Name**: Mrugank Jadhav
- **Email**: mjadhav1@binghamton.edu
- **B-Number**: B00972210

- **Name**: Riddhi Jaju
- **Email**: rjaju1@binghamton.edu
- **B-Number**: B00970879

- **Name**: Shivani Bhatti
- **Email**: sbhatti1@binghamton.edu
- **B-Number**: B00979226

- **Name**: Avani Phase
- **Email**: aphase1@binghamton.edu
- **B-Number**: B00979185

## Programming Language

- **Language**: Python
- **Tested on Virtual Machine**: 128.226.29.104

## Execution Instructions

To execute the program, follow these instructions:

### Encoding

1. Open a terminal or command prompt.
2. Navigate to the directory containing the Python program.
3. Run the following command to perform data collection: 
   
        cd Scrapper
        python3 scheduler.py

4. The program collects data from Reddit every 5 minutes and stores it
   in the postgres database.

5. The program also collects data from Spotify every 10 hours and 
   stores it in the postgres database.

## Code Overview

The Python program `scheduler.py` contains the following functions:

1. **Data Collection**: The program collects data from Reddit using the
   Reddit API and Spotipy API. The data is stored in the postgres 
   database.

2. **Error Handling**: The program handles errors and exceptions 
   gracefully while collecting data from Reddit and Spotify.

3. **Logging**: The program logs the errors and exceptions on the 
   terminal.

4. **Postgres Database**: The program uses the postgres database to 
   store the data collected from Reddit and Spotify. There are multiple
    tables in the database to store the data.
    - `artist_albums`: Stores the albums of the artists.
    - `artist_top_tracks`: Stores the top tracks of the artists.
    - `reddit_comments`: Stores the comments from Reddit.
    - `reddit_posts`: Stores the posts from Reddit.
    - `reddit_job_stats`: Stores the statistics of the Reddit job.
    - `spotify_job_stats`: Stores the statistics of the Spotify job.
    - `spotify_artists`: Stores the artists from Spotify.
    - `track_audio_features`: Stores the audio features of the tracks.

5. **Program Files**: The program contains the following files:
    - `scheduler.py`: The main program file.
    - `redditCrawler.py`: The file contains the functions to collect 
      data from Reddit.
    - `spotifyCrawler.py`: The file contains the functions to collect 
      data from Spotify.
    - `.env`: The file contains the environment variables.

6. **Main Execution**: The `scheduler.py` file contains the main 
   execution of the program. The program collects data from Reddit and 
   Spotify using Reddit and Spotify scripts.

7. **Project Report**: The project report is saved as `CS515_project1_report.pdf`.