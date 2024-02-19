import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd

def get_top_data():
    # Set up Spotify client with OAuth authentication
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id='99a9a7d2fc134fc9bdc9dd8bf0bb2c1f',
                                                   client_secret='263c642dd57b4f43baa44186a32149f5',
                                                   redirect_uri='http://localhost:3000',
                                                   scope='user-top-read'))

    # Retrieve top tracks
    top_tracks = sp.current_user_top_tracks(limit=10, time_range='short_term')
    track_data = [{'name': track['name'], 'artists': ', '.join([artist['name'] for artist in track['artists']])} for track in top_tracks['items']]

    # Retrieve top artists
    top_artists = sp.current_user_top_artists(limit=10, time_range='short_term')
    artist_data = [{'name': artist['name']} for artist in top_artists['items']]

    # Retrieve top albums
    top_albums = sp.current_user_top_tracks(limit=10, time_range='short_term')
    album_data = [{'name': album['album']['name']} for album in top_albums['items']]
    
    # Convert data to DataFrame
    tracks_df = pd.DataFrame(track_data)
    artists_df = pd.DataFrame(artist_data)
    albums_df = pd.DataFrame(album_data)

    # Save DataFrames as CSV files
    tracks_df.to_csv('s3://jasdeep-airflow-bucket/top_tracks.csv', index=False)
    artists_df.to_csv('s3://jasdeep-airflow-bucket/top_artists.csv', index=False)
    albums_df.to_csv('s3://jasdeep-airflow-bucket/top_albums.csv', index=False)

if __name__ == "__main__":
    get_top_data()
