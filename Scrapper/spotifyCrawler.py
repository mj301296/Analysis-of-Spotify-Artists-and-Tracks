import psycopg2
import time
import requests
from datetime import date, datetime, timedelta
import os
from dotenv import load_dotenv

# Load the environment variables from the .env file
load_dotenv()

# Spotify Web API Credentials (from your Spotify Developer Dashboard)

client_ids = os.getenv("SPOTIFY_IDS")
client_secrets = os.getenv("SPOTIFY_SECRETS")
client_id_list = client_ids.split(",")
client_secret_list = client_secrets.split(",")
client_id = client_id_list[0]
client_secret = client_secret_list[0]
current_index = 0

auth_url = "https://accounts.spotify.com/api/token"

data = {
    "grant_type": "client_credentials",
    "client_id": client_id,
    "client_secret": client_secret,
}

db_settings = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
}

auth_response = requests.post(auth_url, data=data)
access_token = auth_response.json().get("access_token")
base_url = "https://api.spotify.com/v1/"
headers = {"Authorization": "Bearer {}".format(access_token)}


# Generate new token with next credentials incase API limit is hit
def generate_token():
    global current_index
    global headers
    current_index = (current_index + 1) % len(client_id_list)
    client_id = client_id_list[current_index]
    client_secret = client_secret_list[current_index]
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    auth_response = requests.post(auth_url, data=data)
    access_token = auth_response.json().get("access_token")
    headers = {"Authorization": "Bearer {}".format(access_token)}


def create_table():
    try:
        # conn=sqlite3.connect('spotify.db')
        conn = psycopg2.connect(**db_settings)

        c = conn.cursor()
        c.execute(
            """
                    CREATE TABLE IF NOT EXISTS spotify_artists(
                    artist_id TEXT PRIMARY KEY,
                    artist_name TEXT,
                    music_genre TEXT,
                    artist_popularity INTEGER
                    );
            """
        )

        c.execute(
            """
                    CREATE TABLE IF NOT EXISTS artist_albums(
                    album_id TEXT PRIMARY KEY,
                    album_name TEXT,
                    album_release_date TEXT,
                    available_markets TEXT,
                    artist_id TEXT,
                    class_mhs TEXT,
                    confidence REAL
                    );
            """
        )

        c.execute(
            """
                    CREATE TABLE IF NOT EXISTS artist_top_tracks(
                    track_id TEXT PRIMARY KEY,
                    track_name TEXT,
                    track_release_date TEXT,
                    available_markets TEXT,
                    track_popularity TEXT,
                    artist_id TEXT,
                    album_id TEXT,
                    class_mhs TEXT,
                    confidence REAL
                    );
                """
        )

        c.execute(
            """
                    CREATE TABLE IF NOT EXISTS track_audio_features(
                        track_id TEXT PRIMARY KEY,
                        acousticness REAL,
                        danceability REAL,
                        energy REAL,
                        instrumentalness REAL,
                        liveness REAL,
                        loudness REAL,
                        speechiness REAL,
                        tempo REAL,
                        valence REAL
                    );
                """
        )
        c.execute(
            """
                CREATE TABLE IF NOT EXISTS spotify_job_stats(
                    id SERIAL PRIMARY KEY,
                    job_start DATE,
                    job_end DATE,
                    spotify_artists_added INTEGER,
                    artist_albums_added INTEGER,
                    artist_top_tracks_added INTEGER,
                    track_audio_features_added INTEGER    
                );
        """
        )
        c.execute("SELECT COUNT(*) FROM spotify_artists;")
        initial_spotify_artists_count = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM artist_albums;")
        initial_artist_albums_count = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM artist_top_tracks;")
        initial_artist_top_tracks_count = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM track_audio_features;")
        initial_track_audio_features_count = c.fetchone()[0]
        conn.commit()
        job_start = datetime.now()

        get_artist_ids(conn)
        update_stats(
            initial_spotify_artists_count,
            initial_artist_albums_count,
            initial_artist_top_tracks_count,
            initial_track_audio_features_count,
            job_start,
            conn,
        )

        # db_display(conn)
    except psycopg2.Error as e:
        print("Error while creating tables:", e)


def get_moderation_score(album_name, max_retries=5, retry_delay=2):
    api_url = "https://api.moderatehatespeech.com/api/v1/moderate/"
    api_key = "338362504c21fcaea123f925f8db987f"  # Replace with your actual API key

    headers = {
        "Content-Type": "application/json",
    }

    data = {
        "token": api_key,
        "text": album_name,
    }

    for attempt in range(1, max_retries + 1):
        response = requests.post(api_url, json=data, headers=headers)

        try:
            response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)

            # Check if the response contains valid JSON
            if response.text.strip():  # Non-empty response
                try:
                    result = response.json()

                    if (
                        "response" in result
                        and result["response"] == "No text string passed"
                    ):
                        # print(f"API Request Failed for {album_name}: {result['response']}")
                        return "NA", 0.0
                    else:
                        return result.get("class", "normal"), result.get(
                            "confidence", 0.0
                        )
                except json.decoder.JSONDecodeError as json_err:
                    print(f"JSON Decode Error: {json_err}")
                    return "jsondecodeerror", 0.0
            else:
                # print(f"Empty JSON Response for {album_name}")
                return "emptyjson", 0.0

        except requests.exceptions.HTTPError as errh:
            print(f"HTTP Error: {errh}")
        except requests.exceptions.RequestException as err:
            print(f"Request Exception: {err}")

        print(f"Attempt {attempt} failed. Retrying in {retry_delay} seconds...")
        time.sleep(retry_delay)

    # All attempts failed, return 'tryagain' and 0.0
    # print(f"All {max_retries} attempts failed for {album_name}. Returning 'tryagain' and 0.0.")
    return "tryagain", 0.0


def get_artist_ids(conn):
    featured_playlists_endpoint = "browse/featured-playlists/?limit=50"
    featured_playlists_url = "".join([base_url, featured_playlists_endpoint])
    try:
        # print("getting artist id")
        response = requests.get(featured_playlists_url, headers=headers)

        playlists = response.json().get("playlists").get("items")

        playlist_ids = set()
        for pl in playlists:
            playlist_id = pl.get("id")
            playlist_ids.add(playlist_id)

        artist_ids = set()
        for p_id in playlist_ids:
            pr = requests.get(
                base_url + "playlists/{}/tracks".format(p_id), headers=headers
            )
            pr_data = pr.json()
            if pr_data:
                playlist_data = pr_data.get("items")
                for tr in playlist_data:
                    track = tr.get("track")
                    if track:
                        artists = track.get("artists")
                        for artist in artists:
                            artist_id = artist.get("id")
                            artist_ids.add(artist_id)

        artist_ids_list = list(artist_ids)
        get_artists_albums(artist_ids_list, conn)
        get_artists_top_tracks(artist_ids_list, conn)
        while artist_ids_list:
            if len(artist_ids_list) > 50:
                current_request = artist_ids_list[:50]
                chunk = ",".join(artist_ids_list[:50])
                get_artists_genres(chunk, conn)
                artist_ids_list = artist_ids_list[50:]
            else:
                current_request = artist_ids_list
                artist_ids_list = []

        chunk = ",".join(artist_ids_list[:50])
    except Exception as e:
        if response.status_code == 429:  # Rate limit hit
            print(f"Rate limit hit for client ID: {headers.get('Authorization')}")
            # Switch to the next client ID
            generate_token()
            # Retry the request
            get_artist_ids(conn)
        else:
            print("Error while fetching artist IDs:", e)


def get_artists_genres(chunk, conn):
    try:
        # print("getting artist genres")
        ar = requests.get(base_url + "artists?ids={}".format(chunk), headers=headers)
        artists_data = ar.json().get("artists")
        for artist in artists_data:
            artist_temp = artist.get("id")
            genre_data = artist.get("genres")
            genre_string = ""
            for genre in genre_data:
                genre_string = genre_string + genre + ","
            artist_name = artist.get("name")
            artist_popularity = artist.get("popularity")
            c = conn.cursor()
            c.execute(
                "INSERT INTO spotify_artists(artist_id,artist_name,music_genre,artist_popularity) "
                "VALUES (%s, %s, %s, %s) "
                "ON CONFLICT (artist_id) DO NOTHING",
                (artist_temp, artist_name, genre_string, artist_popularity),
            )
            conn.commit()
    except Exception as e:
        if ar.status_code == 429:  # Rate limit hit
            print(f"Rate limit hit for client ID: {headers.get('Authorization')}")
            # Switch to the next client ID
            generate_token()
            # Retry the request
            get_artists_genres(chunk, conn)
        else:
            print("Error while fetching artist genres:", e)


def get_artists_albums(chunk, conn):
    try:
        for ch in chunk:
            ar = requests.get(
                base_url + "artists/{}/albums".format(ch), headers=headers
            )
            print(ar)
            ar_data = ar.json()
            if ar_data:
                albums_data = ar_data.get("items")
                for album in albums_data:
                    album_id = album.get("id")
                    album_name = album.get("name")
                    album_release_date = album.get("release_date")
                    available_markets = album.get("available_markets")
                    market_string = ""
                    for market in available_markets:
                        market_string = market_string + market + ","
                    toxicity_class, confidence = get_moderation_score(album_name)
                    c = conn.cursor()
                    # c.execute('INSERT INTO artist_albums VALUES (%,?,?,?,?)',
                    #         (album_id,album_name,album_release_date,market_string,ch))
                    c.execute(
                        "INSERT INTO artist_albums(album_id, album_name, album_release_date,available_markets,artist_id, class_mhs, confidence) "
                        "VALUES (%s, %s, %s, %s,%s, %s %s) "
                        "ON CONFLICT (album_id) DO NOTHING",
                        (
                            album_id,
                            album_name,
                            album_release_date,
                            market_string,
                            ch,
                            toxicity_class,
                            confidence,
                        ),
                    )
                    conn.commit()
    except Exception as e:
        if ar.status_code == 429:  # Rate limit hit
            print(f"Rate limit hit for client ID: {headers.get('Authorization')}")
            # Switch to the next client ID
            generate_token()
            # Retry the request
            get_artists_albums(chunk, conn)
        else:
            print("Error while fetching artist albums:", e)


def get_artists_top_tracks(chunk, conn):
    track_ids = set()
    try:
        for ch in chunk:
            print("getting artist top tracks")
            at = requests.get(
                base_url + "artists/{}/top-tracks?market=US".format(ch), headers=headers
            )
            at_data = at.json()
            if at_data:
                tracks_data = at_data.get("tracks")
                for track in tracks_data:
                    track_id = track.get("id")
                    track_ids.add(track_id)
                    track_name = track.get("name")
                    available_markets = track.get("available_markets")
                    track_popularity = track.get("popularity")
                    album_data = track.get("album")
                    album_id = album_data.get("id")
                    album_release_date = album_data.get("release_date")
                    toxicity_class, confidence = get_moderation_score(track_name)
                    c = conn.cursor()
                    c.execute(
                        "INSERT INTO artist_top_tracks(track_id,track_name,track_release_date ,available_markets,track_popularity,artist_id,album_id, class_mhs, confidence) "
                        "VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s) "
                        "ON CONFLICT (track_id) DO NOTHING",
                        (
                            track_id,
                            track_name,
                            album_release_date,
                            available_markets,
                            track_popularity,
                            ch,
                            album_id,
                            toxicity_class,
                            confidence,
                        ),
                    )
                    conn.commit()

        track_ids_list = list(track_ids)

        while track_ids_list:
            if len(track_ids_list) > 100:
                current_request = track_ids_list[:100]
                parts = ",".join(track_ids_list[:100])
                get_audio_features(parts, conn)
                track_ids_list = track_ids_list[100:]
            else:
                current_request = track_ids_list
                track_ids_list = []
        parts = ",".join(track_ids_list[:100])
    except Exception as e:
        if at.status_code == 429:  # Rate limit hit
            print(f"Rate limit hit for client ID: {headers.get('Authorization')}")
            # Switch to the next client ID
            generate_token()
            # Retry the request
            get_artists_top_tracks(chunk, conn)
        else:
            print("Error while fetching top tracks:", e)


def get_audio_features(parts, conn):
    try:
        print("getting audio")
        ar = requests.get(
            base_url + "audio-features?ids={}".format(parts), headers=headers
        )
        features = ar.json().get("audio_features")
        if features:
            for artist in features:
                if artist:
                    acousticness = artist.get("acousticness")
                    danceability = artist.get("danceability")
                    energy = artist.get("energy")
                    instrumentalness = artist.get("instrumentalness")
                    liveness = artist.get("liveness")
                    loudness = artist.get("loudness")
                    speechiness = artist.get("speechiness")
                    tempo = artist.get("tempo")
                    valence = artist.get("valence")
                    id = artist.get("id")
                    c = conn.cursor()
                    c.execute(
                        "INSERT INTO track_audio_features(track_id,acousticness,danceability,energy,instrumentalness,liveness,loudness,speechiness,tempo,valence) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s,%s,%s,%s) "
                        "ON CONFLICT (track_id) DO NOTHING",
                        (
                            id,
                            acousticness,
                            danceability,
                            energy,
                            instrumentalness,
                            liveness,
                            loudness,
                            speechiness,
                            tempo,
                            valence,
                        ),
                    )
                    conn.commit()
    except Exception as e:
        if ar.status_code == 429:  # Rate limit hit
            print(f"Rate limit hit for client ID: {headers.get('Authorization')}")
            # Switch to the next client ID
            generate_token()
            # Retry the request
            get_audio_features(chunk, conn)
        else:
            print("Error while fetching audio features:", e)


def update_stats(
    initial_spotify_artists_count,
    initial_artist_albums_count,
    initial_artist_top_tracks_count,
    initial_track_audio_features_count,
    job_start,
    conn=None,
):
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM spotify_artists;")
    final_spotify_artists_count = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM artist_albums;")
    final_artist_albums_count = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM artist_top_tracks;")
    final_artist_top_tracks_count = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM track_audio_features;")
    final_track_audio_features_count = c.fetchone()[0]
    job_end = datetime.now()
    spotify_artists_added = final_spotify_artists_count - initial_spotify_artists_count
    artist_albums_added = final_artist_albums_count - initial_artist_albums_count
    artist_top_tracks_added = (
        final_artist_top_tracks_count - initial_artist_top_tracks_count
    )
    track_audio_features_added = (
        final_track_audio_features_count - initial_track_audio_features_count
    )

    c.execute(
        "INSERT INTO spotify_job_stats(job_start, job_end, spotify_artists_added,artist_albums_added,artist_top_tracks_added,track_audio_features_added) "
        "VALUES (%s, %s, %s, %s,%s,%s)",
        (
            job_start,
            job_end,
            spotify_artists_added,
            artist_albums_added,
            artist_top_tracks_added,
            track_audio_features_added,
        ),
    )
    conn.commit()


if __name__ == "__main__":
    print("Running spotify scrapper....")
    create_table()
