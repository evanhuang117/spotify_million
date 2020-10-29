# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import os
import pandas as pd
import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import re
import matplotlib.pyplot as plt


def main():
    # authorize the user
    auth_manager = SpotifyClientCredentials()
    sp = spotipy.Spotify(auth_manager=auth_manager)

    data_dir = 'spotify_million_playlist_dataset/data/'
    playlists = []
    for file in sorted(os.scandir(data_dir), key=lambda e: e.name):
        print("processing slice: " + str(file))
        data = json.load(open(file.path))
        playlists.append(pd.DataFrame(data['playlists']))
        break

    # combine all of the playlists into a single dataframe
    playlists_frame = pd.concat(playlists)
    # print head of playlists dataframe
    print(playlists_frame.head())
    for i, playlist in playlists_frame.iterrows():
        print(playlist['name'])
        # processSongFeatures(playlist, sp)
        processAnalyses(playlist, sp)
        break
    # print(json_frames[0].iloc[0].loc['tracks'])


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


# this function creates a multi-index dataframe
# - level 1 is the category of the analysis
# - level 2 is the song id for the data

def processAnalyses(playlist, sp):
    # get the songs from the first playlist
    songs = playlist['tracks']

    analyses = {}
    # collect analyses of songs, dict of (song id : analysis)
    # pd.set_option('display.max_columns', None)
    for i, song in enumerate(songs):
        song_id = re.sub('spotify:track:', '', song['track_uri'])
        print('analyzing: ' + song['track_name'])
        a = sp.audio_analysis(track_id=song_id)
        # remove some useless crap
        a.pop('meta')
        analyses[song_id] = a
        break
    #track_frame = pd.DataFrame(track_frame.values(), index=track_frame.keys())

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
            print(norm)

    # song_analyses_frame maps a song to a dataframe containing a
    # categorized analysis of the song
    song_analyses_frame = {}
    for cat_name, analysis_data_by_song in category_frames.items():
        for i, data in enumerate(analysis_data_by_song):
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
    category_tables_frame.to_html('frames.html')
    print(category_tables_frame.groupby(level=1).first())

    # export the data
    """export_dir = playlist['name'] + '-' + playlist['id']
    if not os.path.isdir(export_dir):
        os.mkdir(export_dir)

    print(category_tables.keys())
    for category, table in category_tables.items():
        print(category)
        idx = pd.IndexSlice
        table.to_csv(export_dir + '/' + category + '.csv')
        #print(table.loc[idx['4W8iitrK5csxU1kqBeT5Js']])

    category_tables_frame.to_csv(export_dir + '/analysis_category_tables.csv')
    """


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
