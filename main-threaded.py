import logging
import os
import sys
import time
import datetime
from configparser import ConfigParser
from tagging import Tagging
from mongoclient import get_database
import pymongo
import json
import threading
from videotagging import VideoData
from fileops import listdirs, listimages, listvideos, \
    get_image_md5, get_image_content, get_video_content, get_video_content_md5

# initialize logger
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
logging.getLogger('PIL').setLevel(logging.ERROR)
logging.debug("logging started")
logger = logging.getLogger(__name__)

# read config
config = ConfigParser()
config.read('config.ini')
subdivs = json.loads(config.get('properties', 'subdivs'))
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
videocollection.create_index([('content_md5', pymongo.TEXT)], name='content_md5_index')
videocollection.create_index('md5')
videocollection.create_index('vision_tags')

# Initialize variables
tagging = Tagging(config)
imagecount = 0
videocount = 0
foldercount = 0
imagecount_lock = threading.Lock()
videocount_lock = threading.Lock()

# Names of likelihood from google.cloud.vision.enums
likelihood_name = (
    "UNKNOWN",
    "Very unlikely",
    "Unlikely",
    "Possible",
    "Likely",
    "Very likely",
)


def create_mongoimageentry(image_content, im_md5, image_array, relpath_array, is_screenshot, subdiv):
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
        safe = tagging.get_explicit(image_binary=image_content)
        mongo_entry = {
            "md5": im_md5,
            "vision_tags": tags,
            "vision_text": text[0],
            "explicit_detection": [{"adult": f"{likelihood_name[safe.adult]}",
                                    "medical": f"{likelihood_name[safe.medical]}",
                                    "spoofed": f"{likelihood_name[safe.spoof]}",
                                    "violence": f"{likelihood_name[safe.violence]}",
                                    "racy": f"{likelihood_name[safe.racy]}"}],
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


def create_mongovideoentry(video_content, video_content_md5, vidpath_array, relpath_array, subdiv):
    videoobj = VideoData()
    videoobj.video_vision_all(video_content)
    mongo_entry = {
        "content_md5": video_content_md5,
        "vision_tags": videoobj.labels,
        "vision_text": videoobj.text,
        "vision_transcript": videoobj.transcripts,
        "explicit_detection": videoobj.pornography,
        "path": vidpath_array,
        "subdiv": subdiv,
        "relativepath": relpath_array,
    }
    return mongo_entry


def mongo_processimagefolder(workingdir, workingcollection, is_screenshot, subdiv):
    global imagecount, imagecount_lock
    rootdir = config.get('divs', subdiv)
    workingimages = listimages(workingdir, process_images)
    for imagepath in workingimages:
        with imagecount_lock:
            imagecount += 1
        im_md5 = get_image_md5(imagepath)
        relpath = os.path.relpath(imagepath, rootdir)
        # if MD5 is not in MongoDB
        if workingcollection.find_one({"md5": im_md5}, {"md5": 1}) is None:
            image_content = get_image_content(imagepath)
            imagepath_array = [imagepath]
            relpath_array = [relpath]
            mongo_entry = create_mongoimageentry(image_content, im_md5, imagepath_array, relpath_array,
                                                 is_screenshot, subdiv)
            workingcollection.insert_one(mongo_entry)
            logger.info("Added new entry in MongoDB for image %s \n", imagepath)
        # if MD5 is in MongoDB
        else:
            # if path is not in MongoDB
            if workingcollection.find_one({"md5": im_md5, "relativepath": relpath},
                                          {"md5": 1, "relativepath": 1}) is None:
                workingcollection.update_one({"md5": im_md5},
                                             {"$addToSet": {"path": imagepath, "relativepath": relpath}})
                logger.info("Added path in MongoDB for duplicate image %s", imagepath)
            # if path is in MongoDB
            else:
                logger.info("Image %s is in MongoDB", imagepath)


def mongo_processvideofolder(workingdir, subdiv):
    rootdir = config.get('divs', subdiv)
    global videocount, videocount_lock
    workingvideos = listvideos(workingdir, process_videos)
    for videopath in workingvideos:
        with videocount_lock:
            videocount += 1
        video_content_md5 = str(get_video_content_md5(videopath))
        relpath = os.path.relpath(videopath, rootdir)
        # if content MD5 is not in Mongo
        if videocollection.find_one({"content_md5": video_content_md5}, {"content_md5": 1}) is None:
            try:
                logger.info("Processing video %s", relpath)
                videopath_array = [videopath]
                video_content = get_video_content(videopath)
                relpath_array = [relpath]
                mongo_entry = create_mongovideoentry(video_content, video_content_md5,
                                                     videopath_array, relpath_array, subdiv)
                logger.info("Generated MongoDB entry: %s", mongo_entry)
                # noinspection PyUnresolvedReferences
                videocollection.insert_one(mongo_entry)
                logger.info("Added new entry in MongoDB for video %s \n", videopath)
                continue
            # TODO: make this catch a more specific error
            except OSError as e:
                logger.error("Network error %s processing %s", e, relpath)
                continue
        # if content MD5 is in MongoDB
        else:
            # if path is not in MongoDB
            if videocollection.find_one({"content_md5": video_content_md5, "relativepath": relpath},
                                        {"content_md5": 1, "relativepath": 1}) is None:

                videocollection.update_one({"content_md5": video_content_md5},
                                           {"$addToSet": {"path": videopath, "relativepath": relpath}})
                logger.info("Added path in MongoDB for duplicate video %s", videopath)
                continue
            # if path is in MongoDB
            else:
                logger.info("Video %s is in MongoDB", videopath)
                continue


def main():
    global foldercount
    start_time = time.time()
    for div in subdivs:
        rootdir = config.get('divs', div)
        allfolders = listdirs(rootdir)
        for _ in allfolders:  # spawn thread per entry here
            workingdir = allfolders.pop(0)
            foldercount += 1
            if process_images:
                if workingdir.lower().find("screenshot") != -1:
                    is_screenshot = 1
                    workingcollection = screenshotcollection
                else:
                    is_screenshot = 0
                    workingcollection = collection
                t = threading.Thread(target=mongo_processimagefolder, args=[workingdir, workingcollection,
                                                                            is_screenshot, div])
                t.start()
            if process_videos:
                t = threading.Thread(target=mongo_processvideofolder, args=[workingdir, div])
                t.start()

        # Wait until all threads exit
        for thread in threading.enumerate():
            if thread.daemon:
                continue
            try:
                thread.join()
            except RuntimeError as err:
                if 'cannot join current thread' in err.args[0]:
                    # catches main thread
                    continue
                else:
                    raise
            elapsed_time = time.time() - start_time
            final_time = str(datetime.timedelta(seconds=elapsed_time))
            logger.info("All entries processed. Root divs: %s, Folder count: %s", subdivs, foldercount)
            print(imagecount, "images and ", videocount, "videos processed.")
            print("Processing took ", final_time)
            break


if __name__ == "__main__":
    main()