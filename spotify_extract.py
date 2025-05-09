import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
    client_id = os.environ.get('Client_id')
    client_secret = os.environ.get('Client_secret')

    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
    playlist_link = "https://open.spotify.com/playlist/5ABHKGoOzxkaa28ttQV9sE"
    playlist_URI = playlist_link.split("/")[-1]
    tracks = sp.playlist_tracks(playlist_URI)
    #print(tracks)

    client = boto3.client('s3')
    filename = "spotify_raw_" + str(datetime.now()) + ".json"
    
    client.put_object(
        Bucket='spotifybucket000',
        Key='raw_data/to_processed/' + filename,
        Body=json.dumps(tracks))
    return {
        'statusCode': 200,
        'body': json.dumps(tracks)
    }
