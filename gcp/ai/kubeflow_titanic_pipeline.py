from kfp import dsl, compiler
from kfp.dsl import Input, Output, Model, Dataset, InputPath, OutputPath, Metrics, Artifact

from typing import NamedTuple
import google.auth
import google.cloud.aiplatform as aip
from uuid import uuid4
from google_cloud_pipeline_components.v1.custom_job import (create_custom_training_job_from_component)

CREDENTIAL_PATH = ""
CREDENTIALS, PROJECT_ID = google.auth.load_credentials_from_file(CREDENTIAL_PATH)
SERVICE_ACCOUNT = CREDENTIALS.service_account_email

ROOT_BUCKET = "gs://?"
STAGING_BUCKET = f"{ROOT_BUCKET}/staging"
REGION = "us-central1"

PIPELINE_NAME = "titanic"
DISPLAY_NAME = PIPELINE_NAME + uuid4().hex

# Storage Location donde se almacenan los outputs.
PIPELINE_ROOT = f"{ROOT_BUCKET}/pipeline_root/{PIPELINE_NAME}" 
PACKAGE_PATH = f"./compiles/{PIPELINE_NAME}.yaml"

BASE_PYTHON_IMAGE = "python:3.10"
PRE_BUILD_CONTAINER = "us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-3:latest"


aip.init(
    project=PROJECT_ID,
    staging_bucket=STAGING_BUCKET,
    credentials=CREDENTIALS,
    service_account=SERVICE_ACCOUNT
)

@dsl.component(base_image=BASE_PYTHON_IMAGE,
               packages_to_install=["pandas==2.2.1", "requests==2.31.0"])
def create_dataset(output_dataset: Output[Dataset]):
    import requests
    import json
    import pandas as pd
    
    response = requests.get(url="https://github.com/datasciencedojo/datasets/blob/master/titanic.csv")
    data = json.loads(response.content)["payload"]["blob"]["csv"]
    df = pd.DataFrame.from_records(data[1:], columns=data[0])
    
    with open(output_dataset.path, "w") as f:
        df.to_csv(f, index=False)
    
    
@dsl.component(base_image=BASE_PYTHON_IMAGE,
               packages_to_install=["pandas==2.2.1", "scikit-learn==1.4.0"])
def preprocess(input_dataset: Input[Dataset], output_dataset: Output[Dataset]):
    import pandas as pd
    from sklearn.preprocessing import StandardScaler, MinMaxScaler
    
    # Get pandas df
    with open(input_dataset.path, "r") as f:
        df = pd.read_csv(f)
    
    # Init scaler
    standard_scaler = StandardScaler()
    minmax_scaler = MinMaxScaler()
    
    # Drop duplicates and columns
    df = df.drop_duplicates()
    df = df.drop(columns=["PassengerId", "SibSp", "Ticket", "Cabin", "Name"])
    
    # Fill and remove NA
    df["Age"] = df["Age"].fillna(df["Age"].median())
    df = df.dropna(axis=0)
    
    # Transform columns
    df["Sex"] = df["Sex"].map(lambda element: 1 if element == "male" else 0)
    encoded_embarked = pd.get_dummies(df["Embarked"], prefix="Embarked_", drop_first=False)
    df["Age_Scaler"] = minmax_scaler.fit_transform(df[["Age"]])
    df["Fare_Scaler"] = standard_scaler.fit_transform(df[["Fare"]])
    columns = ["Survived", "Pclass", "Parch", "Sex"]
    
    output_df = pd.concat([df[columns], encoded_embarked], axis=1)
    output_df.columns = output_df.columns.str.lower()
    
    with open(output_dataset.path, "w") as f:
        output_df.to_csv(f, index=False)
    

@dsl.component(base_image=BASE_PYTHON_IMAGE,
               packages_to_install=["pandas==2.2.1", "scikit-learn==1.4.0"])
def train(input_dataset: Input[Dataset], 
          model: Output[Model],
          metrics: Output[Metrics],
          test_dataset: Output[Dataset]):
    import pandas as pd
    import pickle
    import logging
    from sklearn.model_selection import train_test_split
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import (accuracy_score, recall_score, precision_score)
    
    logging.getLogger().setLevel(logging.INFO)
    
    # Get pandas df
    with open(input_dataset.path, "r") as f:
        df = pd.read_csv(f)
    
    y = df.pop("survived")
    X = df
    X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=0, train_size=0.8, stratify=y)
    
    logging.info("Entrenamos el modelo LogisticRegression")
    log_reg = LogisticRegression()
    log_reg.fit(X_train, y_train)
    
    accuracy = accuracy_score(y_test, log_reg.predict(X_test))
    recall = recall_score(y_test, log_reg.predict(X_test))
    precision = precision_score(y_test, log_reg.predict(X_test))
    
    metrics.log_metric("model", "LogisticRegression")
    metrics.log_metric("accuracy", round(accuracy, 2))
    metrics.log_metric("recall", round(recall, 2))
    metrics.log_metric("precision", round(precision, 2))
    
    model.path = model.path + ".pkl"
    with open(model.path, "wb") as f:
        pickle.dump(log_reg, f)
        
    test_df = X_test
    test_df["survived"] = y_test
    
    with open(test_dataset.path, "w") as f:
        test_df.to_csv(f, index=False)
        
@dsl.component(base_image=BASE_PYTHON_IMAGE,
               packages_to_install=["google-cloud-aiplatform==1.43.0"])
def deploy_model(
    model: Input[Model],
    project: str,
    region: str,
    display_name: str,
    container: str,
    vertex_endpoint: Output[Artifact],
    vertex_model: Output[Model]
):
    from google.cloud import aiplatform

    aiplatform.init(project=project, location=region)

    deployed_model = aiplatform.Model.upload(
        display_name=display_name,
        artifact_uri = model.uri.replace("model.pkl", ""),
        serving_container_image_uri=container
    )
    
    endpoint = deployed_model.deploy(machine_type="e2-standard-4")

    vertex_endpoint.uri = endpoint.resource_name
    vertex_model.uri = deployed_model.resource_name

@dsl.pipeline(name=PIPELINE_NAME, pipeline_root=PIPELINE_ROOT)
def main(display_name: str,
         project: str = PROJECT_ID,
         region: str = REGION,
         container: str = PRE_BUILD_CONTAINER,
         service_account: str = SERVICE_ACCOUNT):
    from google_cloud_pipeline_components.v1.model import (ModelUploadOp)
    from google_cloud_pipeline_components.v1.endpoint import (EndpointCreateOp, ModelDeployOp)
    from google_cloud_pipeline_components.v1.vertex_notification_email import (VertexNotificationEmailOp)
    from google_cloud_pipeline_components.types.artifact_types import UnmanagedContainerModel
    
    notification_task = VertexNotificationEmailOp(recipients=["JCamatta@coa.com.ar"])
    with dsl.ExitHandler(exit_task=notification_task):
        
        create_dataset_task = create_dataset()
        preprocess_task = preprocess(input_dataset=create_dataset_task.outputs["output_dataset"])
        train_task = train(input_dataset=preprocess_task.outputs["output_dataset"])
        deploy_model_task = deploy_model(
            model=train_task.outputs["model"],
            project=project,
            region=region,
            display_name=display_name,
            container=container,
        )
        

compiler.Compiler().compile(pipeline_func=main, package_path=PACKAGE_PATH)

job = aip.PipelineJob(
    display_name=DISPLAY_NAME,
    template_path=PACKAGE_PATH,
    pipeline_root=PIPELINE_ROOT,
    location=REGION,
    enable_caching=True,
    parameter_values={
        "display_name": PIPELINE_NAME,
        "region": REGION,
        "container": PRE_BUILD_CONTAINER,
        "service_account": SERVICE_ACCOUNT,
        "project": PROJECT_ID
    }
)

job.submit(service_account=SERVICE_ACCOUNT)