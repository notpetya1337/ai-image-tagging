import logging
import io
import os
import sys
from configparser import ConfigParser
from tagging import Tagging
from PIL import Image
import hashlib
from mongoclient import get_database
import pymongo

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
mongodownloadscollection = config.get('storage', 'mongodownloadscollection')
mongoartcollection = config.get('storage', 'mongoartcollection')
mongovideocollection = config.get('storage', 'mongovideocollection')
process_videos = config.getboolean('storage', 'process_videos')


currentdb = get_database()
collection = currentdb[mongocollection]
screenshotcollection = currentdb[mongoscreenshotcollection]
downloadcollection = currentdb[mongodownloadscollection]
artcollection = currentdb[mongoartcollection]
videocollection = currentdb[mongovideocollection]
collection.create_index([('md5', pymongo.TEXT)], name='md5_index', unique=True)
collection.create_index('vision_tags')
screenshotcollection.create_index([('md5', pymongo.TEXT)], name='md5_index', unique=True)
downloadcollection.create_index([('md5', pymongo.TEXT)], name='md5_index', unique=True)
downloadcollection.create_index('vision_tags')
artcollection.create_index([('md5', pymongo.TEXT)], name='md5_index', unique=True)
artcollection.create_index('vision_tags')
videocollection.create_index([('md5', pymongo.TEXT)], name='md5_index', unique=True)
videocollection.create_index('vision_tags')

videoextensions = (".mp4", ".webm", ".mov", ".mkv")
imageextensions = (".png", ".jpg", ".gif", ".jpeg")

if subdiv.find("screenshots") != -1:
    is_screenshot = 1
    workingcollection = screenshotcollection
elif subdiv.find("downloads") != -1:
    is_screenshot = 0
    workingcollection = downloadcollection
elif subdiv.find("art") != -1:
    is_screenshot = 0
    workingcollection = artcollection
else:
    is_screenshot = 0
    workingcollection = collection

workingcollection = currentdb["testcollection"]
rootdir = r"C:\Users\Petya\Dowsnloads\Discord"


# list all subdirectories in a given folder
def listdirs(folder):
    internallist = [folder]
    for root, directories, files in os.walk(folder):
        for directory in directories:
            internallist.append(os.path.join(root, directory))
    return internallist


# list all images in a given folder
def listimages(subfolder):
    internallist = []
    for file in os.listdir(subfolder):
        if file.endswith(imageextensions):
            imagepath = os.path.join(subfolder, file)
            internallist.append(imagepath)
    return internallist


def listvideos(subfolder):
    internallist = []
    if not process_videos:
        logger.info("Not processing videos")
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


def get_video_content(video_path):
    video = io.open(video_path, 'rb')
    return video.read()


def get_md5(image_path):
    try:
        im = Image.open(image_path)
        return hashlib.md5(im.tobytes()).hexdigest()
    except OSError as e:
        logger.warning(e)
        return "corrupt"
    except SyntaxError as e:
        logger.warning(e)
        return "corrupt"


def get_video_md5(video_path, blocksize=2**20):
    m = hashlib.md5()
    try:
        with open(video_path, "rb") as file:
            while True:
                buf = file.read(blocksize)
                if not buf:
                    break
                m.update(buf)
        return m.hexdigest()
    except OSError:
        return "corrupt"
    except SyntaxError:
        return "corrupt"


# define folder and image lists globally
imagelist = []
tagging = Tagging(config)
allfolders = listdirs(rootdir)


for document in list(workingcollection.find()):
    for relpath in document["relativepath"]:
        logger.info("Processing relative path")
        docmd5 = document["md5"]
        logger.info("Doc MD5 is %s", docmd5)
        if relpath.endswith(videoextensions):
            logger.info("Video path is %s", relpath)
            try:
                logger.info("Video MD5 is %s", get_video_md5(os.path.join(rootdir, relpath)))
            except Exception as error:
                logger.error("Error is %s", error)
        if relpath.endswith(imageextensions):
            logger.info("Image path is %s", relpath)
            fullpath = os.path.join(rootdir, relpath)
            logger.info("Full path: %s", fullpath)
            filefound = os.path.isfile(fullpath)
            logger.info("File found status: %s", filefound)
            if filefound:
                try:
                    # TODO: check file MD5s
                    pic_md5 = get_md5(os.path.join(rootdir, relpath))
                    logger.info("Image MD5 is %s", pic_md5)
                    if pic_md5 == docmd5:
                        logger.info("MD5 match for image %s", path)
                    else:
                        logger.warning("MD5 mismatch for image file %s. Local hash is %s, MongoDB hash is %s", path,
                                       pic_md5, docmd5)
                except Exception as error:
                    logger.error("Error is %s", error)
            else:
                logger.info("Pulling path record", "{'_id': %s}, { '$pull': {'relativepath': { '$in': %s}}}",
                            document["_id"], path)
                workingcollection.update_one({'_id': document["_id"]},
                                             {'$pull': {'relativepath': {'$in': [relpath]}}})
                # pull path and relpath from Mongo with ID
        else:
            logger.warning("File %s not recognized as image or video file", relpath)

    for path in document["path"]:
        logger.info("Processing full path")
        docmd5 = document["md5"]
        logger.info("Doc MD5 is %s", docmd5)
        if path.endswith(videoextensions):
            logger.info("Video path is %s", path)
            try:
                logger.info("Video MD5 is %s", get_video_md5(path))
            except Exception as error:
                logger.error("Error is %s", error)

        if path.endswith(imageextensions):
            logger.info("Image path is %s", path)
            filefound = os.path.isfile(path)
            logger.info("File found status: %s", filefound)
            if filefound:
                try:
                    pic_md5 = get_md5(os.path.join(rootdir, path))
                    logger.info("Image MD5 is %s", pic_md5)
                    if pic_md5 == docmd5:
                        logger.info("MD5 match for image %s", path)
                    else:
                        logger.warning("MD5 mismatch for image file %s. Local hash is %s, MongoDB hash is %s", path,
                                       pic_md5, docmd5)
                except Exception as error:
                    logger.error("Error is %s", error)
                # now check MD5 against Mongo entry
            else:
                logger.info("Pulling path record", "{'_id': %s}, { '$pull': {'relativepath': "
                                                   "{'$in': %s}}}", document["_id"], path)
                workingcollection.update_one({'_id': document["_id"]}, {'$pull': {'relativepath': {'$in': [path]}}})
        else:
            logger.warning("File %s not recognized as image or video file", path)
