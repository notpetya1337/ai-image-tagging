import json
import logging
import threading
import time
from configparser import ConfigParser

from watchdog.events import PatternMatchingEventHandler
from watchdog.observers import Observer

from dependencies.mongoclient import get_database
from main_threaded import process_video, process_image

# read config
config = ConfigParser()
config.read("config.ini")
subdiv = config.get("properties", "subdiv")
rootdir = config.get("divs", subdiv)
subdivs = json.loads(config.get("properties", "subdivs"))
mongocollection = config.get("storage", "mongocollection")
mongovideocollection = config.get("storage", "mongovideocollection")

# initialize DBs
currentdb = get_database()
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

# folders = []
# for div in subdivs:
#     folders.append(config.get('divs', div))

exit_event = threading.Event()


# def signal_handler(signum, frame):
#     exit_event.set()


class OnMyWatch:
    # Set the directory on watch
    def __init__(self, folder, div):
        self.folder = folder
        self.div = div
        self.observer = Observer()

    def run(self):
        event_handler = Handler(self.div)
        self.observer.schedule(event_handler, self.folder, recursive=True)
        self.observer.start()
        while True:
            time.sleep(5)
            if exit_event.is_set():
                self.observer.stop()
                logger.info("Observer Stopped")
                break


class Handler(PatternMatchingEventHandler):
    def __init__(self, div):
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

    def on_any_event(self, event):
        if event.is_directory:
            return None
        elif event.event_type == "created":
            # Event is created, you can process it now
            print("Watchdog received created event - % s" % event.src_path)
        elif event.event_type == "modified":
            # process_image will trigger this, so be careful to avoid loops
            print("Watchdog received modified event - % s" % event.src_path)
        elif event.event_type == "moved":
            # Event is moved, you can process it now
            logger.info(
                "Watchdog received moved event - %s moved to %s",
                event.src_path,
                event.dest_path,
            )
            path = event.dest_path
            logger.info("Running command on %s", path)
            if path.endswith(videoextensions):
                time.sleep(5)
                logger.info("Processing video %s", path)
                t = threading.Thread(process_video(path, subdiv))
                t.start()
            elif path.endswith(imageextensions):
                time.sleep(5)
                logger.info("Processing image %s", path)
                t = threading.Thread(
                    process_image(
                        path,
                        is_screenshot=False,
                        rootdir=None,
                        subdiv=self.div,
                        workingcollection=collection,
                    )
                )
                t.start()
        # else:
        #     print("Event type is ", event.event_type)


if __name__ == "__main__":
    logger.info("Watching %s", subdivs)
    for i in subdivs:
        folder = config.get("divs", i)
        threading.Thread(target=OnMyWatch(folder, i).run).start()
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
