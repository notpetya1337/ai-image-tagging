import os
from moviepy.editor import VideoFileClip
import logging
import sys
import time
import datetime
from configparser import ConfigParser
from dependencies.fileops import listdirs, listvideos

# initialize logger
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
logging.getLogger('PIL').setLevel(logging.ERROR)
logging.debug("logging started")
logger = logging.getLogger(__name__)

config = ConfigParser()
config.read('config.ini')
subdiv = config.get('properties', 'subdiv')
rootdir = config.get('divs', subdiv)

allfolders = listdirs(rootdir)


def convert_video_to_mp4(file_path):
    # Get the file name and extension
    folder_path, origfilename = os.path.split(file_path)
    name, file_ext = os.path.splitext(origfilename)
    mp4_filename = f"{name}.mp4"
    clip = VideoFileClip(folder_path + "\\" + origfilename)
    if os.path.isfile(folder_path + "\\" + "converted_" + mp4_filename):
        logger.warning("File %s\\%s already converted", folder_path, mp4_filename)
    else:
        clip.write_videofile(folder_path + "\\" + "converted_" + mp4_filename)
    clip.close()
    os.remove(folder_path + "\\" + origfilename)
    logger.info(f"Conversion successful. {name} converted to {mp4_filename}.")


def main():
    videocount = 0
    start_time = time.time()
    while True:
        if allfolders:
            workingdir = allfolders.pop(0)
            workingvideos = listvideos(workingdir, True)
            for videopath in workingvideos:
                videocount += 1
                videoname, videoext = os.path.splitext(videopath)
                if videoext in [".webm", ".mkv"]:
                    convert_video_to_mp4(videopath)
        else:
            elapsed_time = time.time() - start_time
            final_time = str(datetime.timedelta(seconds=elapsed_time))
            logger.info("All entries processed. Root folder: %s Folder list: %s", rootdir, allfolders)
            print(videocount, "videos processed.")
            print("Processing took ", final_time)
            break


if __name__ == "__main__":
    main()
