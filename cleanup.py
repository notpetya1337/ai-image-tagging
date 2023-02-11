import logging
import io
import os
import sys
import time
import datetime
from configparser import ConfigParser
from tagging import Tagging
import sqlite3
from PIL import Image
import hashlib
from mongoclient import get_database
import pymongo
from videotagging import VideoData

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
sqldb = config.get('storage', 'sqlitedb')
mongocollection = config.get('storage', 'mongocollection')
mongoscreenshotcollection = config.get('storage', 'mongoscreenshotcollection')
mongodownloadscollection = config.get('storage', 'mongodownloadscollection')
mongoartcollection = config.get('storage', 'mongoartcollection')
mongovideocollection = config.get('storage', 'mongovideocollection')
process_videos = config.getboolean('storage', 'process_videos')

# initialize DBs
# TODO: turn these into functions and call them
con = sqlite3.connect(sqldb)
cur = con.cursor()
cur.execute("""CREATE TABLE IF NOT EXISTS media 
    (md5 TEXT NOT NULL, 
    relativepath TEXT, is_screenshot BOOLEAN NOT NULL CHECK (is_screenshot IN (0, 1)), subdiv TEXT);""")
cur.execute(
    "CREATE TABLE IF NOT EXISTS screenshots (md5 INTEGER NOT NULL PRIMARY KEY, vision_text TEXT, names TEXT);")
cur.execute("CREATE INDEX IF NOT EXISTS md5_idx ON media (md5);")
con.commit()
logger.info('DB initialized')
# except:
# logger.error('Unable to initialize DB')
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

# list all subdirectories in a given folder
def listdirs(folder):
    internallist = [folder]
    for root, directories, files in os.walk(folder):
        for directory in directories:
            internallist.append(os.path.join(root, directory))
    return internallist


# list all images in a given folder
def listimages(subfolder):
    imageextensions = (".png", ".jpg", ".gif", ".jpeg")
    internallist = []
    for file in os.listdir(subfolder):
        if file.endswith(imageextensions):
            imagepath = os.path.join(subfolder, file)
            internallist.append(imagepath)
    return internallist


def listvideos(subfolder):
    imageextensions = (".mp4", ".webm", ".mov", ".mkv")
    internallist = []
    if process_videos == False:
        logger.info("Not processing videos")
        return internallist
    for file in os.listdir(subfolder):
        if file.endswith(imageextensions):
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
    except OSError as error:
        logger.warning(error)
        return "corrupt"
    except SyntaxError as error:
        logger.warning(error)
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

#workingcollection = mongovideocollection

for document in list(workingcollection.find()):
    #print(document)
    print("\ndoc _id:", document["_id"])
    imageextensions = (".png", ".jpg", ".gif", ".jpeg")
    videoextensions = (".mp4", ".webm", ".mov", ".mkv")
    for doc in document["path"]:
        if doc.endswith(videoextensions):
            print("Doc path is", doc)
            try:
                print(get_video_md5(os.path.join(rootdir, doc)))
            except Exception as error:
                print("Error is ", error)
                print("Not found")

        if doc.endswith(imageextensions):
            print("Doc is ", doc)
            try:
                print(get_md5(os.path.join(rootdir, doc)))
            except Exception as error:
                print("Error is ", error)
                print("Not found")
            # select path and md5, search fs for path and md5
