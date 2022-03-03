#
# Usage:
#
# Prerequisites:
# - pandas
#
# Author: sha
#
# Changes:
# 2022-03-02
# - initial version

import argparse
import pandas as pd
import pytube
from pytube import exceptions
from moviepy.video.io.VideoFileClip import AudioFileClip
import os
import math
from datetime import datetime
import youtube_dl


def create_subdir_structure(log, nmax_files=5000):
    """
    creates subdir structure and enriches log to store the subdir IDs to the corresponding files to download
    :param log: pandas df including the files
    :param nmax_files: upper limit of files per subdir
    :return: log enriched by
    """
    nsubdirs = math.ceil(len(log) / nmax_files)
    # create subdirs
    log['subdir'] = 1
    for i in range(0, nsubdirs):
        subdir_path = f"{output_path}/{i}"
        if not os.path.exists(subdir_path):
            # Create a new directory because it does not exist
            os.makedirs(subdir_path)
        # subdir ID to log
        log.iloc[i*nmax_files:i*nmax_files+nmax_files, :]['subdir'] = i
    # add subdir ID to log
    return log


def download_from_frame(log):
    """
    download files from log
    :param log: input log with files to download
    :return:
    """
    log.apply(lambda x: download(x['yt_id'], os.sep.join((output_path, str(x['subdir'])))), axis=1)


def download(yt_id, out_dir):
    """downloader function for youtube mp3s, returns the filename and writes to given target directory"""
    out_path = f'{os.sep.join((out_dir, yt_id))}.mp4'
    if os.path.exists(out_path):
        print("%s was already downloaded" % out_path)
    else:
        """
        try:
            print(yt_uri)
            youtube_dl.YoutubeDL().download([yt_uri])
            print(f"Downloading {yt_id}")
        """
        try:
            yt_uri = 'https://www.youtube.com/watch?v=' + yt_id
            mp4File = pytube.YouTube(yt_uri).streams.filter().first().download("tmp", filename="tmp")
            clip = AudioFileClip(mp4File)
            clip.write_audiofile(out_path)
            print("Downloaded: " + out_path)
            clip.close()
        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(e)
    return datetime.now()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='download YT Mp3s from given CSV')
    parser.add_argument('-f', '--from_csv', type=str, help='filename of csv', default='')
    parser.add_argument('-nsubdirs', '--no_of_subdirs', type=int, help='upper limit of files per subdir', default=5000)
    args = parser.parse_args()
    # create log
    log = pd.read_csv(args.from_csv, sep=';')
    log['downloaded'] = None
    # create dir to store videos
    output_path = f'data/{args.from_csv}'
    if not os.path.exists(output_path):
        # Create a new directory because it does not exist
        os.makedirs(output_path)
    # subdir structure
    log = create_subdir_structure(log, args.no_of_subdirs)
    log.to_csv(f'log_{args.from_csv}', sep=';', index=False)
    # now download
    download_from_frame(log)
    log.to_csv(f'log_{args.from_csv}', sep=';', index=False)
