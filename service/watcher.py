import json
import logging
import threading
import time
from configparser import ConfigParser
import os
import pymongo

import exiftool
from watchdog.events import PatternMatchingEventHandler
from watchdog.observers import Observer

from dependencies.fileops import get_image_md5
from old.main_threaded import process_image, process_video
from old.tagwriter_threaded import getimagetags, writeimagetags


# read config
config = ConfigParser()
config.read("config.ini")
subdiv = config.get("properties", "subdiv")
rootdir = config.get("divs", subdiv)
subdivs = json.loads(config.get("properties", "subdivs"))
connectstring = config.get('storage', 'connectionstring')
mongodbname = config.get('storage', 'mongodbname')
mongocollection = config.get("storage", "mongocollection")
mongovideocollection = config.get("storage", "mongovideocollection")
google_credentials = config.get("image-recognition", "google-credentials")
google_project = config.get("image-recognition", "google-project")
tags_backend = config.get("image-recognition", "backend")

# initialize DBs
currentdb = pymongo.MongoClient(connectstring)[mongodbname]
collection = currentdb[mongocollection]
videocollection = currentdb[mongovideocollection]

# initialize logger
logging.basicConfig(level=logging.INFO)
logging.getLogger("PIL").setLevel(logging.ERROR)
logging.debug("logging started")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

imageextensions = (".png", ".jpg", ".gif", ".jpeg", ".webp")
videoextensions = (".mp4", ".webm", ".mov", ".mkv")

exit_event = threading.Event()


def imagehandler(path, workingcollection, div, et):
    process_image(path, is_screenshot=False, rootdir=None, subdiv=div, workingcollection=collection)
    md5 = get_image_md5(path)
    tags, text = getimagetags(md5, workingcollection, is_screenshot=False)
    writeimagetags(path, tags, text, et)


class OnMyWatch:
    # Set the directory on watch
    def __init__(self, targetfolder, div):
        self.folder = targetfolder
        self.div = div
        self.observer = Observer()
        self.et = exiftool.ExifToolHelper(logger=logging.getLogger(__name__).setLevel(logging.INFO), encoding="utf-8")

    def run(self):
        event_handler = Handler(self.div, self.et)
        self.observer.schedule(event_handler, self.folder, recursive=True)
        self.observer.start()
        while True:
            time.sleep(5)
            if exit_event.is_set():
                self.observer.stop()
                self.et.terminate()
                logger.info("Observer Stopped")
                break


class Handler(PatternMatchingEventHandler):
    def __init__(self, div, et):
        super(Handler, self).__init__(
            patterns=[
                "*.png",
                "*.jpg",
                "*.gif",
                "*.jpeg",
                "*.webp",
                "*.mp4",
                "*.webm",
                "*.mov",
                "*.mkv",
            ],
            ignore_directories=True,
            case_sensitive=True,
        )
        self.div = div
        self.et = et

    def on_any_event(self, event):
        if event.is_directory:
            return None
        elif event.event_type == "created":
            # Event is created, you can process it now
            logger.info("Watchdog received created event - %s", event.src_path)
        elif event.event_type == "modified":
            # process_image will trigger this, so be careful to avoid loops
            logger.info("Watchdog received modified event - %s", event.src_path)
        elif event.event_type == "moved":
            # Event is moved, you can process it now
            logger.info("Watchdog received moved event - %s moved to %s", event.src_path, event.dest_path)
            path = event.dest_path
            if path.endswith(videoextensions) and not event.src_path.endswith("_tmp"):
                time.sleep(5)
                logger.info("Processing video %s", path)
                t = threading.Thread(process_video(path, subdiv))
                t.start()
            elif path.endswith(imageextensions) and not event.src_path.endswith("_tmp"):
                time.sleep(5)
                logger.info("Processing image %s", path)
                t = threading.Thread(imagehandler(path, collection, subdiv, self.et))
                t.start()
        # else:
        #     print("Event type is ", event.event_type)


if __name__ == "__main__":
    logger.info("Watching %s", subdivs)
    for i in subdivs:
        folder = config.get("divs", i)
        threading.Thread(target=OnMyWatch(folder, i).run).start()
    if os.name == 'posix':
        import sdnotify
        n = sdnotify.SystemdNotifier()
        n.notify("READY=1")
    # Wait until all threads exit
    for thread in threading.enumerate():
        if thread.daemon:
            continue
        try:
            thread.join()
        except RuntimeError as err:
            if "cannot join current thread" in err.args[0]:
                # catches main thread
                continue
            else:
                raise
        except KeyboardInterrupt:
            exit_event.set()
            logger.info("Keyboard interrupt received, exiting")
