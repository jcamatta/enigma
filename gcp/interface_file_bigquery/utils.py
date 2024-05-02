import os
from typing import Any

from pydantic import BaseModel

from dim_models import raw as dim_raw
from fact_models import raw as fact_raw


def construct_model_name(file: str) -> str:
    table_name = os.path.basename(file).split(".")[0]
    table_name = "".join([word.lower().title() for word in table_name.split("_")])
    return table_name + "Model"


def select_model(class_name: str) -> BaseModel:
    if (
        "fct" in class_name.lower()
        or "fact" in class_name.lower()
        or "example" in class_name.lower()
    ):
        selected_class = getattr(fact_raw, class_name)
    elif "lkp" in class_name.lower() or "dim" in class_name.lower():
        selected_class = getattr(dim_raw, class_name)
    else:
        error = f"No existe un modelo para el class_name = {class_name}"
        raise ValueError(error)
    return selected_class


def format_record(record: str, delimiter: str = ",", model: Any = None) -> list | BaseModel:
    output = [element.strip() for element in record.split(delimiter)]
    if model:
        fields = list(model.model_fields.keys())
        data = {fields[k]: output[k] for k in range(len(output)) if output[k]}
        return model(**data)
    return output