import argparse
import io
import os
from collections import namedtuple
from configparser import ConfigParser
from google.cloud import videointelligence
import typing

# read config
config = ConfigParser()
config.read('config.ini')
path = r"C:\Users\Petya\Downloads\Discord\ReactionPics\razorfist chair spin fuck you I was right.mp4"
google_credentials = config.get('image-recognition', 'google-credentials')
google_project = config.get('image-recognition', 'google-project')
tags_backend = config.get('image-recognition', 'backend')
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = google_credentials
os.environ["GOOGLE_CLOUD_PROJECT"] = google_project


class VideoData:
    # def __init__(self, text: str, labels: str, labels_category: str, transcripts: str, video_binary):
    def __init__(self):
        # self.video_binary = video_binary
        self.text = []
        self.labels = []
        self.labels_category = []
        self.transcripts = []
        self.video_binary = []

    def video_vision_all(self, video_binary):
        video_client = videointelligence.VideoIntelligenceServiceClient()
        features = [videointelligence.Feature.LABEL_DETECTION, videointelligence.Feature.TEXT_DETECTION,
                    videointelligence.Feature.SPEECH_TRANSCRIPTION]
        config = videointelligence.SpeechTranscriptionConfig(language_code="en-US", enable_automatic_punctuation=True)
        video_context = videointelligence.VideoContext(speech_transcription_config=config)
        operation = video_client.annotate_video(
            request={"features": features, "input_content": video_binary, "video_context": video_context}
        )
        print("\nProcessing video for all annotations:")
        result = operation.result(timeout=600)
        print("\nFinished processing.")

        # print("Transcription results: ")
        for annotation_result in result.annotation_results:
            for speech_transcription in annotation_result.speech_transcriptions:
                for alternative in speech_transcription.alternatives:
                    # print("Transcript: {}".format(alternative.transcript))
                    self.transcripts.append(alternative.transcript)

        # print("Text results: ")
        for r in result.annotation_results:
            for text_annotation in r.text_annotations:
                # print("Text: {}".format(text_annotation.text))
                self.text.append(text_annotation.text)

        # print("Label results")
        # if result.annotation_results is None:  # doesn't actually work because of default responses
            # print("No label results.")
        for r in result.annotation_results:
            shot_labels = r.shot_label_annotations
            for i, shot_label in enumerate(shot_labels):
                # print("Video label description: {}".format(shot_label.entity.description))
                self.labels.append(shot_label.entity.description)
                for category_entity in shot_label.category_entities:
                    # print("\tLabel category description: {}".format(category_entity.description))
                    self.labels_category.append(category_entity.description)
            # print("\n")

        # return VideoData(self.text, self.labels, self.labels_category, self.transcripts)

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
            "md5": vid_md5,
            "vision_tags": videoobj.labels,
            "vision_text": videoobj.text,
            "vision_transcript": videoobj.transcripts,
            "path": path,
            "subdiv": subdiv,
            "relativepath": relpath_array,
        }
    print(mongo_entry)

if __name__ == "__main__":
    main()

