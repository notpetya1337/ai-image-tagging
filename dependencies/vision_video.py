import io
import logging
import os
from configparser import ConfigParser

from google.cloud import videointelligence

# read config
config = ConfigParser()
config.read("config.ini")
path = r"placeholder"
google_credentials = config.get("image-recognition", "google-credentials")
google_project = config.get("image-recognition", "google-project")
tags_backend = config.get("image-recognition", "backend")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = google_credentials
os.environ["GOOGLE_CLOUD_PROJECT"] = google_project

logger = logging.getLogger(__name__)


class VideoData:
    def __init__(self):
        self.text = []
        self.labels = []
        self.labels_category = []
        self.transcripts = []
        self.video_binary = []
        self.pornography = []

    def video_vision_all(self, video_binary):
        video_client = videointelligence.VideoIntelligenceServiceClient()
        features = [
            videointelligence.Feature.LABEL_DETECTION,
            videointelligence.Feature.TEXT_DETECTION,
            videointelligence.Feature.SPEECH_TRANSCRIPTION,
            videointelligence.Feature.EXPLICIT_CONTENT_DETECTION,
        ]
        vision_config = videointelligence.SpeechTranscriptionConfig(
            language_code="en-US", enable_automatic_punctuation=True
        )
        video_context = videointelligence.VideoContext(
            speech_transcription_config=vision_config
        )
        operation = video_client.annotate_video(
            request={
                "features": features,
                "input_content": video_binary,
                "video_context": video_context,
            }
        )
        print("\nProcessing video for all annotations:")
        result = operation.result(timeout=600)
        print("\nFinished processing.")

        for annotation_result in result.annotation_results:
            for speech_transcription in annotation_result.speech_transcriptions:
                for alternative in speech_transcription.alternatives:
                    self.transcripts.append(alternative.transcript)
        for r in result.annotation_results:
            for text_annotation in r.text_annotations:
                self.text.append(text_annotation.text)
        for r in result.annotation_results:
            shot_labels = r.shot_label_annotations
            for i, shot_label in enumerate(shot_labels):
                self.labels.append(shot_label.entity.description)
                for category_entity in shot_label.category_entities:
                    self.labels_category.append(category_entity.description)
        for frame in result.annotation_results[0].explicit_annotation.frames:
            likelihood = videointelligence.Likelihood(frame.pornography_likelihood)
            self.pornography.append("pornography: {}".format(likelihood.name))
        # remove duplicates
        self.pornography = list(dict.fromkeys(self.pornography))

    def video_vision_explicit(self, video_binary):
        video_client = videointelligence.VideoIntelligenceServiceClient()
        features = [videointelligence.Feature.EXPLICIT_CONTENT_DETECTION]
        vision_config = videointelligence.SpeechTranscriptionConfig(
            language_code="en-US", enable_automatic_punctuation=True
        )
        video_context = videointelligence.VideoContext(
            speech_transcription_config=vision_config
        )
        operation = video_client.annotate_video(
            request={
                "features": features,
                "input_content": video_binary,
                "video_context": video_context,
            }
        )
        logger.info("Processing video for all annotations:")
        result = operation.result(timeout=600)
        logger.info("Finished processing.")
        for frame in result.annotation_results[0].explicit_annotation.frames:
            likelihood = videointelligence.Likelihood(frame.pornography_likelihood)
            self.pornography.append("pornography: {}".format(likelihood.name))
        # remove duplicates
        self.pornography = list(dict.fromkeys(self.pornography))


subdiv = "placeholder"
vid_md5 = "placeholder"
relpath_array = "placeholder"


def main():
    with io.open(path, "rb") as movie:
        video_content = movie.read()
    videoobj = VideoData()
    videoobj.video_vision_all(video_content)
    print(videoobj)
    mongo_entry = {
        "content_md5": vid_md5,
        "vision_tags": videoobj.labels,
        "vision_text": videoobj.text,
        "vision_transcript": videoobj.transcripts,
        "explicit_detection": videoobj.pornography,
        "path": path,
        "subdiv": subdiv,
        "relativepath": relpath_array,
    }
    print(mongo_entry)


if __name__ == "__main__":
    main()
