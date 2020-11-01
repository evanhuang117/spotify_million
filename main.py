# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import os
import pandas as pd
import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import re
import matplotlib.pyplot as plt
import requests
MAX_RETRIES = 5
EXPORT_DIR = 'playlist_frames/'
DATA_DIR = 'spotify_million_playlist_dataset/data/'

def main():
    # authorization
    auth_manager = SpotifyClientCredentials()
    sp = spotipy.Spotify(auth_manager=auth_manager)

    playlists = []
    for file in sorted(os.scandir(DATA_DIR), key=lambda e: e.name):
        print("processing slice: " + str(file.name))
        data = json.load(open(file.path))
        playlists.append(pd.DataFrame(data['playlists']))
        break

    # combine all of the playlists into a single dataframe
    playlists_frame = pd.concat(playlists)
    print(playlists_frame.head())
    for i, playlist in playlists_frame.iterrows():
        print('analyzing: ' + playlist['name'])
        # processSongFeatures(playlist, sp)
        # processAnalyses(playlist, sp, EXPORT_DIR)
        break

    # load dataframes
    for file in sorted(os.scandir(EXPORT_DIR), key=lambda e: e.name):
        print('loading: ' + str(file.name))
        df = pd.read_csv(file, header=[0,1], index_col=0, low_memory=False)

    print(df['Unnamed: 1_level_0'].index)
    print(df.groupby(df['Unnamed: 1_level_0'].index).head())
    group = df.groupby(level=0).get_group('bars')
    print(group.columns.levels)
    #plt.scatter(x=df.groupby('track'))


def processNamesAndIds(playlist, sp):
    songs = playlist['songs']
    id_to_name = {}
    for name, id in songs:
        id_to_name[id] = name
    # create a dataframe of song ids and their names
    song_name_and_id_frame = pd.DataFrame.from_dict(id_to_name, orient='index', columns=['song_name'])
    song_name_and_id_frame.index.name = 'song_id'
    print(song_name_and_id_frame.head())

    # export to the playlist's directory
    export_dir = playlist['name'] + '-' + playlist['id']
    if not os.path.isdir(export_dir):
        os.mkdir(export_dir)

    song_name_and_id_frame.to_csv(export_dir + '/song_names.csv')


def processSongFeatures(playlist, sp):
    songs = playlist['tracks']

    # get features for all songs
    song_features = []
    song_ids = []
    for song in songs:
        # get song id
        song_id = re.sub('spotify:track:', '', song['track_uri'])
        song_ids.append(song_id)
        print('processing: ' + song['track_name'])
        features = sp.audio_features(song_id)[0]
        song_features.append(features)

    # convert features into dataframe by song id
    features_by_id = pd.DataFrame(song_features, index=song_ids)
    features_by_id.index.name = 'song_id'
    print(features_by_id.head())

    # export data
    """export_dir = playlist['name'] + '-' + playlist['id']
    if not os.path.isdir(export_dir):
        os.mkdir(export_dir)

    features_by_id.to_csv(export_dir + '/features.csv')"""


# this function creates a multi-index dataframe and exports it to csv
# - level 1 is the category of the analysis
# - level 2 is the song id for the data

def processAnalyses(playlist, sp, path):
    # get the songs from the first playlist
    songs = playlist['tracks']

    analyses = {}
    # collect analyses of songs, dict of (song id : analysis)
    for i, song in enumerate(songs):
        song_id = re.sub('spotify:track:', '', song['track_uri'])
        # sometimes the api request times out, we'll skip the song if it exceeds the max retries
        for request_attempt in range(MAX_RETRIES):
            print('analyzing: ' + song['track_name'])
            try:
                a = sp.audio_analysis(track_id=song_id)
            except requests.exceptions.ReadTimeout as rto:
                print('request to Spotify timed out for: ' + song['track_name'])
            else:
                break
        else:
            continue
        # remove some useless crap
        a.pop('meta')
        analyses[song_id] = a

    ids = []
    category_frames = {}
    # iterate through all songs and their analyses
    for song_id, a in analyses.items():
        print('processing song: ' + song_id)
        ids.append(song_id)
        # collect the analysis data for each category in the songs' analysis
        for category, vals in a.items():
            print('processing category: ' + category)
            norm = pd.json_normalize(vals)
            # val_list is a list of dataframes
            val_list = category_frames.get(category, [])
            # add the new analysis data for the category
            val_list.append(norm)
            category_frames[category] = val_list

    # song_analyses_frame maps a song to a dataframe containing a
    # categorized analysis of the song
    song_analyses_frame = {}
    for cat_name, analysis_data_by_song in category_frames.items():
        print('combining analysis category: ' + cat_name)
        # data is a DataFrame of the data of a songs analysis for the current category
        for i, data in enumerate(analysis_data_by_song):
            print('combining data for: ' + str(ids[i]))
            # get analysis dict for the current song
            analysis = song_analyses_frame.get(ids[i], {})
            # create a new analysis category and its data
            analysis[cat_name] = data
            # add/update the dict with the song's new analysis
            song_analyses_frame[ids[i]] = analysis

    category_tables = {}
    # concatenate the list of frames in each category to make tables based on song id
    for cat_name, frame_list in category_frames.items():
        category_tables[cat_name] = pd.concat(frame_list, keys=ids)

    # combine the dictionary of categories and their lists of songs
    category_tables_frame = pd.concat(category_tables.values(), keys=category_tables.keys())
    #category_tables_frame.to_html('frames.html')

    # export the data
    file_name = playlist['pid']
    category_tables_frame.to_csv(path + str(file_name) + '.csv')

    # enable to export individual tables for each category, grouped by song
    # not really needed if you use groupby
    """
    export_dir = playlist['name'] + '-' + playlist['pid']
    if not os.path.isdir(export_dir):
        os.mkdir(export_dir)
        
    print(category_tables.keys())
    for category, table in category_tables.items():
        print(category)
        idx = pd.IndexSlice
        table.to_csv(export_dir + '/' + category + '.csv')"""


def batch(item_list, batch_size=1):
    l = len(item_list)
    for ndx in range(0, l, batch_size):
        yield item_list[ndx:min(ndx + batch_size, l)]

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
