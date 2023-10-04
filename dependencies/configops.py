import json
from configparser import ConfigParser


class MainConfig:
    def __init__(self, configpath):
        # read config
        self.config = ConfigParser()
        self.config.read(configpath)
        self.subdivs = json.loads(self.config.get("properties", "subdivs"))
        self.subdivs = json.loads(self.config.get("properties", "subdivs"))
        self.threads = self.config.getint("properties", "threads")
        self.connectstring = self.config.get("storage", "connectionstring")
        self.mongodbname = self.config.get("storage", "mongodbname")
        self.mongocollection = self.config.get("storage", "mongocollection")
        self.mongovideocollection = self.config.get("storage", "mongovideocollection")
        self.mongoscreenshotcollection = self.config.get(
            "storage", "mongoscreenshotcollection"
        )
        self.tags_backend = self.config.get("image-recognition", "backend")
        self.configmodels = json.loads(self.config.get("image-recognition", "models"))
        self.google_credentials = self.config.get(
            "image-recognition", "google-credentials"
        )
        self.google_project = self.config.get("image-recognition", "google-project")
        self.deepbmodelpath = self.config.get("deepb", "model")
        self.deepbtagfile = self.config.get("deepb", "tagfile")
        self.deepbthreshold = self.config.getfloat("deepb", "threshold")
        self.deepbdivs = json.loads(self.config.get("deepb", "deepbdivs"))
        self.logging_level = self.config.get("logging", "loglevel")  # TODO: use this
        self.process_only_new = self.config.get("flags", "process_only_new")
        self.process_videos = self.config.getboolean("flags", "process_videos")
        self.process_images = self.config.getboolean("flags", "process_images")
        self.process_screenshots = self.config.getboolean(
            "flags", "process_screenshots"
        )

    def getdiv(self, div):
        return self.config.get("divs", div)
