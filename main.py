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
collection.create_index([('md5', pymongo.TEXT)], name='md5_index', unique=True)
collection.create_index('vision_tags')
screenshotcollection.create_index([('md5', pymongo.TEXT)], name='md5_index', unique=True)


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
    try:
        im = Image.open(image_path)
        return hashlib.md5(im.tobytes()).hexdigest()
    except OSError:
        return "corrupt"

# define folder and image lists globally
imagelist = []
tagging = Tagging(config)
allfolders = listdirs(rootdir)


def create_mongoentry(image_content, im_md5, image_array, relpath_array, is_screenshot):

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
    logger.info("Generated MongoDB entry: %s", mongo_entry)
    return mongo_entry


def main():
    while True:
        if allfolders:
            workingdir = allfolders.pop(0)
            workingimages = listimages(workingdir)
            for imagepath in workingimages:
                # TODO: check file path and name for "screenshot", update is_screenshot
                if subdiv == "screenshots":
                    is_screenshot = 1
                    workingcollection = screenshotcollection
                else:
                    is_screenshot = 0
                    workingcollection = collection
                im_md5 = get_md5(imagepath)
                relpath = os.path.relpath(imagepath, rootdir)
                md5select = cur.execute("SELECT relativepath FROM media WHERE md5=?", (im_md5,))
                md5check = md5select.fetchone()
                pathselect = cur.execute("SELECT relativepath FROM media WHERE md5=? AND relativepath=?", (im_md5, relpath))
                pathcheck = pathselect.fetchone()
                if pathcheck is None:  # if MD5 and path aren't in SQLite
                    # TODO: if-else isn't the best way to do this
                    if md5check is None:  # if MD5 is not in SQLite
                        if workingcollection.find_one({"md5": im_md5}, {"md5": 1}) is None:  # if MD5 is not in MongoDB
                            image_content = get_image_content(imagepath)
                            imagepath_array = [imagepath]
                            relpath_array = [relpath]
                            mongo_entry = create_mongoentry(image_content, im_md5, imagepath_array, relpath_array, is_screenshot)
                            workingcollection.insert_one(mongo_entry)
                            cur.execute("INSERT INTO media VALUES (?,?,?,?)", (im_md5, relpath, is_screenshot, subdiv))
                            con.commit()
                            logger.info("Added new entry in MongoDB and SQLite for image %s", imagepath)
                            continue
                        else:  # if MD5 is in MongoDB
                            if workingcollection.find_one({"md5": im_md5, "relativepath": relpath},
                                                   {"md5": 1, "relativepath": 1}) is None:  # if path is not in MongoDB
                                workingcollection.update_one({"md5": im_md5},
                                                      {"$addToSet": {"path": imagepath, "relativepath": relpath}})
                                cur.execute("INSERT INTO media VALUES (?,?,?,?)",
                                            (im_md5, relpath, is_screenshot, subdiv))
                                con.commit()
                                logger.info("Added path in MongoDB and SQLite for duplicate image %s", imagepath)
                                continue
                            else:  # if path is in MongoDB
                                logger.warning("Image %s is in MongoDB but not SQLite", imagepath)
                                cur.execute("INSERT INTO media VALUES (?,?,?,?)",
                                            (im_md5, relpath, is_screenshot, subdiv))
                                con.commit()
                                continue
                    else:  # if MD5 but not path is in SQLite
                        if workingcollection.find_one({"md5": im_md5, "relativepath": relpath},
                                                      {"md5": 1,
                                                       "relativepath": 1}) is None:  # if path is not in MongoDB
                            workingcollection.update_one({"md5": im_md5},
                                                         {"$addToSet": {"path": imagepath, "relativepath": relpath}})
                            cur.execute("INSERT INTO media VALUES (?,?,?,?)",
                                        (im_md5, relpath, is_screenshot, subdiv))
                            con.commit()
                            logger.info("Added new entry in MongoDB and SQLite for duplicate path %s", imagepath)
                            continue
                        else:  # if path is in Mongo but not SQL
                            logger.warning("Path for duplicate image %s is in Mongo but not SQL")
                            cur.execute("INSERT INTO media VALUES (?,?,?,?)",
                                        (im_md5, relpath, is_screenshot, subdiv))
                            con.commit()
                            continue
                else:  # if image and path are in SQLite, check Mongo to avoid silent corruption
                    if workingcollection.find_one({"md5": im_md5, "relativepath": relpath},
                                           {"md5": 1, "relativepath": 1}) is None:
                        logger.warning("Image %s is in SQLite but not MongoDB.", imagepath)
                        continue
                    else:
                        logger.info('Image %s is already in MongoDB and SQLite with this path', imagepath)
                        continue
        else:
            logger.error("No folders found. Root folder: %s Folder list: %s", rootdir, allfolders)
            break


main()
