import logging
import sys

import PIL
import numpy as np
import tensorflow as tf


class deepdanbooruModel:
    def __init__(self, threshold, modelpath, tagfile):
        self.logger = logging.getLogger(__name__)
        self.threshold = threshold
        self.modelpath = modelpath
        self.tagfile = tagfile
        self.tags = None
        self.model = self.load_model()

    def load_model(self):
        self.logger.info("Loading model...")
        try:
            model = tf.keras.models.load_model(self.modelpath, compile=False)
        except:
            self.logger.error(
                "Model not in folder. Download it from https://github.com/KichangKim/DeepDanbooru"
            )
            sys.exit()
        with open(self.tagfile, "r") as tags_stream:
            self.tags = np.array(
                [tag for tag in (tag.strip() for tag in tags_stream) if tag]
            )
        self.logger.info("Model and tag list loaded.")
        return model

    def classify_image(self, image_path):
        try:
            image = (
                np.array(PIL.Image.open(image_path).convert("RGB").resize((512, 512)))
                / 255.0
            )
        except IOError as e:
            self.logger.error("Error %s processing %s", e, image_path)
            return "fail", []

        results = self.model.predict(np.array([image])).reshape(self.tags.shape[0])
        result_tags = {}
        for i in range(len(self.tags)):
            if results[i] > self.threshold:
                result_tags[self.tags[i]] = results[i]
        return "success", list(result_tags.keys())
