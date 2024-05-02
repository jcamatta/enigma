import argparse
import io
import logging
from typing import MutableSequence

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.ml.inference.base import ModelHandler, RunInference

from google.cloud import vision
from google.cloud.vision import ImageAnnotatorClient, Feature


PROJECT = ""
IMAGE_PATH_PATTERN = ""

class CloudVisionModelHandler(ModelHandler):
    
    def load_model(self) -> ImageAnnotatorClient:
        client = ImageAnnotatorClient()
        return client
    
    def run_inference(self, 
                      batch: tuple[str, bytes],
                      model: ImageAnnotatorClient, inference_args) -> tuple[str, MutableSequence[vision.AnnotateImageResponse]]:
        
        feature = Feature()
        feature.type_  = Feature.Type.LABEL_DETECTION
        
        image_urls = [image_url for (image_url, image_bytes) in batch]
        
        images = [vision.Image(content=image_bytes) for (_, image_bytes) in batch]
        image_requests = [vision.AnnotateImageRequest(image=image, features=[feature]) for image in images]
        batch_image_request = vision.BatchAnnotateImagesRequest(requests=image_requests)

        responses = model.batch_annotate_images(request=batch_image_request).responses
        return list(zip(image_urls, responses))
        

def read_image(image: fileio.ReadableFile) -> tuple[str, bytes]:
    image_bytes = image.open(mime_type="image/jpeg").read()
    return image.metadata.path, image_bytes


def post_process(inference: tuple[str, MutableSequence[vision.AnnotateImageResponse]]):
    return f"{inference[0]},{','.join([label.description for label in inference[1].label_annotations])}"

def run():
    
    parser = argparse.ArgumentParser()
    
    know_args, beam_args = parser.parse_known_args()
    beam_options = PipelineOptions(
        beam_args,
        direct_num_workers=1,
        project=PROJECT
    )
    
    with beam.Pipeline(options=beam_options) as pipeline:
        read_images = (pipeline 
                    | fileio.MatchFiles(file_pattern=IMAGE_PATH_PATTERN, empty_match_treatment="ALLOW")
                    | fileio.ReadMatches()
                    | beam.Map(read_image))
        
        inferences, error = (read_images
                            | RunInference(model_handler=CloudVisionModelHandler()).with_exception_handling())
        
        (inferences
         | beam.Map(post_process)
         | beam.io.WriteToText(
             file_path_prefix="./output/cloudvision_output",
             file_name_suffix=".txt",
             shard_name_template=""
         ))
    
if __name__ == "__main__":
    # logging.getLogger().setLevel(logging.INFO)
    run()