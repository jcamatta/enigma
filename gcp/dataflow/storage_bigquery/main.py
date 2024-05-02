import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from company_pkg.models import utils as models_utils
from company_pkg.models.dim import raw as dimRawModels

from storage_bigquery import transforms, dofns, utils

def run():
    
    logging.basicConfig(level=logging.INFO)
    
    # Se crea el argumento input
    parser = argparse.ArgumentParser()
    
    # gs://bucket/folder1/folder2/lkp_fechas.txt
    parser.add_argument("--input", required=True, type=str)
    
    known_args, beam_args = parser.parse_known_args()
    beam_options = PipelineOptions(
        beam_args,
        runner="DataflowRunner",
        save_main_session=True,
        pipeline_type_check=True,
        runtime_type_check=False,
        machine_type="e2-standard-2",
        temp_location="gs://bucket/temp",
        prebuild_sdk_container_engine="cloud_build",
        pickle_library="cloudpickle",
        sdk_container_image="us-central1-docker.pkg.dev/project/repo/base_image_name:latest",
    )
    
    project = beam_options.get_all_options()["project"]
    temp_location = beam_options.get_all_options()["temp_location"]
    
    # Se inicia el pipeline
    with beam.Pipeline(options=beam_options) as pipeline:
        
        logging.info(f"Todos los modelos en sigma_coa.dim.raw son: {str(dir(dimRawModels))}")
        
        # Construimos el nombre del modelo asociado al archivo
        logging.info("Obteniendo el model asociado al input")
        model_name = models_utils.construct_model_name(known_args.input)
        # Obtenemos el modelo, esto es, la clase
        model = models_utils.select_model(model_name)
        logging.info(f"El modelo a usar es {model}. Procedemos a realizar el etl...")
        
        # Leemos el archivo
        logging.info("Extraemos la data")
        read_data = pipeline | "ReadInputFile" >> beam.io.ReadFromText(known_args.input, skip_header_lines=int(model.__header__))
        
        # Transformamos
        logging.info("Transformamos la data")
        transforms = (read_data
                        #| "RemoveEmptyRows" >> beam.Filter(utils.remove_empty, delimiter=model.__delimiter__)
                        | "ParseRecordsIntoModel" >> beam.Map(models_utils.format_record, delimiter=model.__delimiter__, model=model)
                        | "ParseModelIntoDict" >> beam.Map(lambda record: record.model_dump())
        )
        
        # Lo almacenamos en bigquery usando el metodo FILE_LOADS.
        logging.info("Almacenamos la data")
        output = (transforms
                    | beam.io.WriteToBigQuery(
                        method="FILE_LOADS",
                        custom_gcs_temp_location=temp_location,
                        project=project,
                        dataset=model.__dataset__,
                        table=model.__tablename__,
                        write_disposition=model.__write_disposition__,
                        create_disposition=model.__create_disposition__,
                        schema=model.bigquery_schema(exclude=["default"]),
                        )
        )
    
if __name__ == "__main__":
    run()