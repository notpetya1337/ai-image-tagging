# Google Cloud tagging for images

## Functionality:
This script takes images from a local folder, processes the images through Google's Vision AI, retrieves AI-generated labels and OCRed text, and writes it to EXIF tags.

Labels are written to the ImageDescription field, while OCRed text is written to UserComment for now.\
Google Vision currently allows 1000 free requests per month and charges $1.50 per 1000 requests afterward.

## Use case:
The Google Photos app allows you to search based on the EXIF "ImageDescription" field. You can also use ElasticSearch as an image search backend, possibly with a custom app to show image previews.

## TODO:
Fix ElasticSearch tag storing\
Add OCR tagging\
Add better validation for returned OCR text\
Add batch processing and/or multithreading (processing a single image currently takes 1-3 seconds)\
Possibly add web GUI