import logging
import io
import os
import sys
import time
import datetime
from configparser import ConfigParser
from tagging import Tagging
from PIL import Image
import hashlib
from mongoclient import get_database
import pymongo
from videotagging import VideoData
import subprocess
import re

# initialize logger
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
logging.getLogger('PIL').setLevel(logging.ERROR)
logging.debug("logging started")
logger = logging.getLogger(__name__)

# read config
config = ConfigParser()
config.read('config.ini')
subdiv = config.get('properties', 'subdiv')
rootdir = config.get('divs', subdiv)
mongocollection = config.get('storage', 'mongocollection')
mongoscreenshotcollection = config.get('storage', 'mongoscreenshotcollection')
mongovideocollection = config.get('storage', 'mongovideocollection')
process_videos = config.getboolean('storage', 'process_videos')
process_images = config.getboolean('storage', 'process_images')


# initialize DBs
currentdb = get_database()
collection = currentdb[mongocollection]
screenshotcollection = currentdb[mongoscreenshotcollection]
videocollection = currentdb[mongovideocollection]
collection.create_index([('md5', pymongo.TEXT)], name='md5_index', unique=True)
collection.create_index('vision_tags')
screenshotcollection.create_index([('md5', pymongo.TEXT)], name='md5_index', unique=True)
# videocollection.create_index([('content_md5', pymongo.TEXT)], name='content_md5_index', unique=True)
videocollection.create_index('md5')
videocollection.create_index('vision_tags')


# list all subdirectories in a given folder
def listdirs(folder):
    internallist = [folder]
    for root, directories, files in os.walk(folder):
        for directory in directories:
            internallist.append(os.path.join(root, directory))
    return internallist


# list all images in a given folder
def listimages(subfolder):
    imageextensions = (".png", ".jpg", ".gif", ".jpeg", ".webp")
    internallist = []
    if not process_images:
        logger.warning("Not processing images")
        return internallist
    for file in os.listdir(subfolder):
        if file.endswith(imageextensions):
            imagepath = os.path.join(subfolder, file)
            internallist.append(imagepath)
    return internallist


def listvideos(subfolder):
    videoextensions = (".mp4", ".webm", ".mov", ".mkv")
    internallist = []
    if not process_videos:
        logger.warning("Not processing videos")
        return internallist
    for file in os.listdir(subfolder):
        if file.endswith(videoextensions):
            videopath = os.path.join(subfolder, file)
            internallist.append(videopath)
    return internallist


# open an image at a given path
def get_image_content(image_path):
    image = open(image_path, 'rb')
    return image.read()


# open a video at a given path
def get_video_content(video_path):
    video = io.open(video_path, 'rb')
    return video.read()


def get_md5(image_path):
    try:
        im = Image.open(image_path)
        return hashlib.md5(im.tobytes()).hexdigest()
    except OSError:
        return "corrupt"
    except SyntaxError:
        return "corrupt"


def get_video_content_md5(video_path):
    try:
        process = subprocess.Popen('cmd /c ffmpeg.exe -i "{vpath}" -map 0:v -f md5 -'.format(vpath=video_path),
                                   shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()
        md5list = re.findall(r"MD5=([a-fA-F\d]{32})", str(out))
        logger.info("Got content MD5 for video %s: %s", video_path, md5list)
        md5 = md5list[0]
    except Exception as e:
        logger.error("Unhandled exception getting MD5 for path %s with ffmpeg: %s", video_path, e)
        md5 = "corrupt"
    return md5


# define folder and image lists globally
imagelist = []
tagging = Tagging(config)
allfolders = listdirs(rootdir)


def create_mongoimageentry(image_content, im_md5, image_array, relpath_array, is_screenshot):
    if is_screenshot == 1:
        text = tagging.get_text(image_binary=image_content)
        mongo_entry = {
            "md5": im_md5,
            "vision_tags": [],
            "vision_text": text[0],
            "path": image_array,
            "subdiv": subdiv,
            "relativepath": relpath_array,
            "is_screenshot": is_screenshot
        }
    elif is_screenshot == 0:
        tags = tagging.get_tags(image_binary=image_content)
        text = tagging.get_text(image_binary=image_content)
        mongo_entry = {
            "md5": im_md5,
            "vision_tags": tags,
            "vision_text": text[0],
            "path": image_array,
            "subdiv": subdiv,
            "relativepath": relpath_array,
            "is_screenshot": is_screenshot
        }
    else:
        logger.error("%s did not match is_screenshot or is_video", relpath_array)
        mongo_entry = ""
    logger.info("Generated MongoDB entry: %s", mongo_entry)
    return mongo_entry


def create_mongovideoentry(video_content, video_content_md5, vidpath_array, relpath_array):
    videoobj = VideoData()
    videoobj.video_vision_all(video_content)
    mongo_entry = {
        "content_md5": video_content_md5,
        "vision_tags": videoobj.labels,
        "vision_text": videoobj.text,
        "vision_transcript": videoobj.transcripts,
        "path": vidpath_array,
        "subdiv": subdiv,
        "relativepath": relpath_array,
    }
    return mongo_entry


def main():
    while True:
        imagecount = 0
        start_time = time.process_time()
        if allfolders:
            workingdir = allfolders.pop(0)
            workingimages = listimages(workingdir)
            workingvideos = listvideos(workingdir)

#######################################################################################################################
            for videopath in workingvideos:
                workingcollection = videocollection
                video_content_md5 = str(get_video_content_md5(videopath))
                relpath = os.path.relpath(videopath, rootdir)

                # if content MD5 is not in Mongo
                if workingcollection.find_one({"content_md5": video_content_md5}, {"content_md5": 1}) is None:
                    try:
                        logger.info("Processing video %s", relpath)
                        videopath_array = [videopath]
                        video_content = get_video_content(videopath)
                        relpath_array = [relpath]
                        mongo_entry = create_mongovideoentry(video_content, video_content_md5,
                                                             videopath_array, relpath_array)
                        logger.info("Generated MongoDB entry: %s", mongo_entry)
                        try:
                            workingcollection.insert_one(mongo_entry)
                        except (pymongo.errors.ServerSelectionTimeoutError, pymongo.errors.AutoReconnect) as e:
                            logger.warning("Connection error: %s", e)
                            time.sleep(10)
                        logger.info("Added new entry in MongoDB for video %s \n", videopath)
                        continue
                    # TODO: make this catch a more specific error
                    except OSError as e:
                        logger.error("Network error %s processing %s", e, relpath)
                        continue

                # if content MD5 is in MongoDB
                else:
                    # if path is not in MongoDB
                    if workingcollection.find_one({"content_md5": video_content_md5, "relativepath": relpath},
                                                  {"content_md5": 1, "relativepath": 1}) is None:

                        workingcollection.update_one({"content_md5": video_content_md5},
                                                     {"$addToSet": {"path": videopath, "relativepath": relpath}})
                        logger.info("Added path in MongoDB for duplicate video %s", videopath)
                        continue
                    # if path is in MongoDB
                    else:
                        logger.info("Video %s is in MongoDB", videopath)
                        continue

#######################################################################################################################
            for imagepath in workingimages:
                imagecount += 1
                if subdiv.find("screenshots") != -1:
                    is_screenshot = 1
                    workingcollection = screenshotcollection
                else:
                    is_screenshot = 0
                    workingcollection = collection
                im_md5 = get_md5(imagepath)
                relpath = os.path.relpath(imagepath, rootdir)

                # if MD5 is not in MongoDB
                if workingcollection.find_one({"md5": im_md5}, {"md5": 1}) is None:
                    image_content = get_image_content(imagepath)
                    imagepath_array = [imagepath]
                    relpath_array = [relpath]
                    mongo_entry = create_mongoimageentry(image_content, im_md5, imagepath_array, relpath_array,
                                                         is_screenshot)
                    try:
                        workingcollection.insert_one(mongo_entry)
                    except (pymongo.errors.ServerSelectionTimeoutError, pymongo.errors.AutoReconnect) as e:
                        logger.warning("Connection error: %s", e)
                        time.sleep(10)
                    logger.info("Added new entry in MongoDB for image %s \n", imagepath)
                    continue
                # if MD5 is in MongoDB
                else:
                    # if path is not in MongoDB
                    if workingcollection.find_one({"md5": im_md5, "relativepath": relpath},
                                                  {"md5": 1, "relativepath": 1}) is None:
                        try:
                            workingcollection.update_one({"md5": im_md5},
                                                         {"$addToSet": {"path": imagepath, "relativepath": relpath}})
                        except (pymongo.errors.ServerSelectionTimeoutError, pymongo.errors.AutoReconnect) as e:
                            logger.warning("Connection error: %s", e)
                            time.sleep(10)
                        logger.info("Added path in MongoDB for duplicate image %s", imagepath)
                        continue
                    # if path is in MongoDB
                    else:
                        logger.info("Image %s is in MongoDB", imagepath)
                        continue

        else:
            elapsed_time = time.process_time() - start_time
            final_time = str(datetime.timedelta(seconds=elapsed_time))
            logger.info("All entries processed. Root folder: %s Folder list: %s", rootdir, allfolders)
            print(imagecount, " media processed.")
            print("Processing took ", final_time)
            break


if __name__ == "__main__":
    main()
