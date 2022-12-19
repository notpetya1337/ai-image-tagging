import logging
import os
import sys
from configparser import ConfigParser
from tagging import Tagging
import sqlite3
from PIL import Image
import hashlib
from mongoclient import get_database
import pymongo

# logger
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
logging.getLogger('PIL').setLevel(logging.ERROR)
logging.debug("logging started")
logger = logging.getLogger(__name__)

config = ConfigParser()
config.read('config.ini')
subdiv = config.get('properties', 'subdiv')
rootdir = config.get('divs', subdiv)
sqldb = config.get('storage', 'sqlitedb')
mongocollection = config.get('storage', 'mongocollection')


# initialize DBs
con = sqlite3.connect(sqldb)
cur = con.cursor()
cur.execute("""CREATE TABLE IF NOT EXISTS media 
    (md5 TEXT NOT NULL, 
    path TEXT, is_screenshot BOOLEAN NOT NULL CHECK (is_screenshot IN (0, 1)), subdiv TEXT);""")
cur.execute(
    "CREATE TABLE IF NOT EXISTS screenshots (md5 INTEGER NOT NULL PRIMARY KEY, vision_text TEXT, names TEXT);")
cur.execute("CREATE INDEX IF NOT EXISTS md5_idx ON media (md5);")
con.commit()
logger.info('DB initialized')
# except:
# logger.error('Unable to initialize DB')
currentdb = get_database()
collection = currentdb[mongocollection]
collection.create_index([('md5', pymongo.TEXT)], name='md5_index', unique=True)
collection.create_index('vision_tags')


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


# open an image at a given path
def get_image_content(image_path):
    image = open(image_path, 'rb')
    return image.read()


def get_md5(image_path):
    # using try/catch to keep PIL debug logs from spamming console, fix this later
    try:
        im = Image.open(image_path)
        return hashlib.md5(im.tobytes()).hexdigest()
    except:
        logger.error("Failed to get hash of =%s", image_path)


# define folder and image lists globally
imagelist = []
tagging = Tagging(config)
allfolders = listdirs(rootdir)


def main():
    while True:
        if allfolders:
            workingdir = allfolders.pop(0)
            workingimages = listimages(workingdir)
            for imagepath in workingimages:
                im_md5 = get_md5(imagepath)
                relpath = os.path.relpath(imagepath, rootdir)
                md5select = cur.execute("SELECT path FROM media WHERE md5=?", (im_md5,))
                sqlcheck = md5select.fetchone()
                pathselect = cur.execute("SELECT path FROM media WHERE md5=? AND path=?", (im_md5, relpath))
                check_path = pathselect.fetchone()
                if sqlcheck is None:
                    # TODO: check file path or config file for "screenshot", maybe use subdiv
                    is_screenshot = 0
                    logger.info('=%s MD5 is not in SQLite database', imagepath)
                    logger.info('opening image=%s', imagepath)
                    image_content = get_image_content(imagepath)
                    logger.info('getting tags for image=%s', imagepath)
                    tags = tagging.get_tags(image_binary=image_content)
                    text = tagging.get_text(image_binary=image_content)
                    print(imagepath, relpath, tags, text)
                    image_array = [imagepath]
                    relpath_array = [relpath]
                    print(image_array)
                    cur.execute("INSERT INTO media VALUES (?,?,?,?)", (im_md5, relpath, is_screenshot, subdiv))
                    con.commit()
                    mongo_entry = {
                        "md5": im_md5,
                        "vision_tags": tags,
                        "vision_text": text[0],
                        "path": image_array,
                        "subdiv": subdiv,
                        "relativepath": relpath_array,
                        "is_screenshot": is_screenshot
                    }
                    mongomd5check = collection.find_one({"md5": im_md5}, {"md5": 1})
                    if mongomd5check is None:
                        collection.insert_one(mongo_entry)
                    else:
                        logger.error("The hash of =%s is already in MongoDB. Your local SQLite database may not be correct.", imagepath)
                        # TODO: this may not be a good idea
                        cur.execute("INSERT INTO media VALUES (?,?,?,?)", (im_md5, relpath, is_screenshot, subdiv))
                        con.commit()
                if check_path is None:
                    # append a path entry
                    # TODO: check MongoDB to see if path exists in any path entry matching that MD5
                    collection.update_one(
                        {"md5": im_md5},
                        {"$addToSet": {"path": imagepath, "relpath": relpath}}
                    )
                    logger.info('Appended path for duplicate, path is =%s', imagepath)
                else:
                    logger.info('=%s is already in MongoDB and SQLite with this path', imagepath)
                    continue
        else:
            print("No folders found", rootdir, allfolders)
            break


main()
