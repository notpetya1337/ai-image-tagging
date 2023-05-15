import io
import os
from configparser import ConfigParser
from google.cloud import videointelligence

# read config
config = ConfigParser()
config.read('config.ini')
path = r"C:\Users\Petya\Downloads\Discord\ReactionPics\razorfist chair spin fuck you I was right.mp4"
google_credentials = config.get('image-recognition', 'google-credentials')
google_project = config.get('image-recognition', 'google-project')
tags_backend = config.get('image-recognition', 'backend')
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = google_credentials
os.environ["GOOGLE_CLOUD_PROJECT"] = google_project

"""Detect labels given a file path."""


def video_vision_labels(video_binary):
    video_client = videointelligence.VideoIntelligenceServiceClient()
    features = [videointelligence.Feature.LABEL_DETECTION]
    vision_config = videointelligence.SpeechTranscriptionConfig(language_code="en-US", enable_automatic_punctuation=True)
    video_context = videointelligence.VideoContext(speech_transcription_config=vision_config)
    operation = video_client.annotate_video(
        request={"features": features, "input_content": video_binary, "video_context": video_context}
    )
    print("\nProcessing video for label annotations:")
    result = operation.result(timeout=180)
    print("\nFinished processing.")

    print("Label results: ")
    # Process video/segment level label annotations
    if result.annotation_results is None:  # doesn't actually work because of default responses
        print("No label results.")
    for r in result.annotation_results:
        shot_labels = r.shot_label_annotations
        for i, shot_label in enumerate(shot_labels):
            print("Video label description: {}".format(shot_label.entity.description))
            for category_entity in shot_label.category_entities:
                print(
                    "\tLabel category description: {}".format(category_entity.description)
                )
        print("\n")

def video_vision_text(video_binary):
    video_client = videointelligence.VideoIntelligenceServiceClient()
    features = [videointelligence.Feature.TEXT_DETECTION]
    vision_config = videointelligence.SpeechTranscriptionConfig(language_code="en-US", enable_automatic_punctuation=True)
    video_context = videointelligence.VideoContext(speech_transcription_config=vision_config)
    operation = video_client.annotate_video(
        request={"features": features, "input_content": video_binary, "video_context": video_context}
    )
    print("\nProcessing video for OCR text annotations:")
    result = operation.result(timeout=180)
    print("\nFinished processing.")

    print("Text results: ")
    for r in result.annotation_results:
        for text_annotation in r.text_annotations:
            print("Text: {}".format(text_annotation.text))
            # print("Confidence: {}".format(text_segment.confidence))

def video_vision_transcription(video_binary):
    video_client = videointelligence.VideoIntelligenceServiceClient()
    features = [videointelligence.Feature.SPEECH_TRANSCRIPTION]
    vision_config = videointelligence.SpeechTranscriptionConfig(language_code="en-US", enable_automatic_punctuation=True)
    video_context = videointelligence.VideoContext(speech_transcription_config=vision_config)
    operation = video_client.annotate_video(
        request={"features": features, "input_content": video_binary, "video_context": video_context}
    )
    print("\nProcessing video for transcription:")
    result = operation.result(timeout=180)
    print("\nFinished processing.")

    print("Transcription results: ")
    for annotation_result in result.annotation_results:
        for speech_transcription in annotation_result.speech_transcriptions:

            # The number of alternatives for each transcription is limited by
            # SpeechTranscriptionConfig.max_alternatives.
            # Each alternative is a different possible transcription
            # and has its own confidence score.
            for alternative in speech_transcription.alternatives:
                # print("Alternative level information:")
                print("Transcript: {}".format(alternative.transcript))


def video_vision_all_mongo(video_binary):
    video_client = videointelligence.VideoIntelligenceServiceClient()
    features = [videointelligence.Feature.LABEL_DETECTION, videointelligence.Feature.TEXT_DETECTION, videointelligence.Feature.SPEECH_TRANSCRIPTION]
    vision_config = videointelligence.SpeechTranscriptionConfig(language_code="en-US", enable_automatic_punctuation=True)
    video_context = videointelligence.VideoContext(speech_transcription_config=vision_config)
    operation = video_client.annotate_video(
        request={"features": features, "input_content": video_binary, "video_context": video_context}
    )

    transcript_results = []
    ocr_results = []
    label_description_results = []
    label_category_results = []

    print("\nProcessing video for all annotations:")
    result = operation.result(timeout=600)
    print("\nFinished processing.")

    print("Transcription results: ")
    for annotation_result in result.annotation_results:
        for speech_transcription in annotation_result.speech_transcriptions:
            for alternative in speech_transcription.alternatives:
                print("Transcript: {}".format(alternative.transcript))
                transcript_results.append(alternative.transcript)

    print("Text results: ")
    for r in result.annotation_results:
        for text_annotation in r.text_annotations:
            print("Text: {}".format(text_annotation.text))
            ocr_results.append(text_annotation.text)

    print("Label results")
    if result.annotation_results is None:  # doesn't actually work because of default responses
        print("No label results.")
    for r in result.annotation_results:
        shot_labels = r.shot_label_annotations
        for i, shot_label in enumerate(shot_labels):
            print("Video label description: {}".format(shot_label.entity.description))
            label_description_results.append(shot_label.entity.description)
            for category_entity in shot_label.category_entities:
                print("\tLabel category description: {}".format(category_entity.description))
                label_category_results.append(category_entity.description)
        print("\n")


def main():
    with io.open(path, "rb") as movie:
        video_content = movie.read()
    video_vision_all_mongo(video_binary=video_content)


if __name__ == "__main__":
    main()
