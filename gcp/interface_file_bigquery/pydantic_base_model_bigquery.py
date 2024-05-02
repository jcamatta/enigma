import datetime
from typing import Any, Sequence, Tuple, get_args

from pydantic import BaseModel, ValidationInfo, field_validator
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefinedType

PYTHON_TYPES_TO_BIG_QUERY = {
    str: "STRING",
    int: "INT64",
    float: "NUMERIC",
    bool: "BOOL",
    dict: "JSON",  # AVRO FILE FORMAT en APACHE_BEAM BIGQUERY WRITE FILE_LOADS no soporta JSON
    datetime.date: "DATE",
    datetime.datetime: "DATETIME",
    datetime.time: "TIME",
    Sequence: "RECORD",
}


class BigqueryBase(BaseModel):
    __tablename__: str
    __dataset__: str = "raw"
    __delimiter__: str = ","
    __header__: bool = True
    __write_disposition__: str = "WRITE_TRUNCATE"  # WRITE_APPEND
    __create_disposition__: str = "CREATE_IF_NEEDED"  # CREATE_NEVER

    @classmethod
    def bigquery_schema(cls, exclude: list = []) -> dict:
        fields = cls.model_fields
        schema = []
        for field, field_info in fields.items():
            schema_field = {}
            dtype, mode, default, description = cls.parse_field_info(field_info)

            # campos obligatorios
            schema_field["name"] = field
            schema_field["type"] = dtype

            if "mode" not in exclude:
                schema_field["mode"] = mode

            # Si el campo tiene definido un valor por defecto ...
            if "default" not in exclude and not isinstance(field_info, PydanticUndefinedType):
                schema_field["default"] = default

            if "description" not in exclude and description:
                schema_field["description"] = description

            schema.append(schema_field)

        return dict(fields=schema)

    @classmethod
    def parse_field_info(cls, field_info: FieldInfo) -> Tuple:
        field_annotation = field_info.annotation  # int - str - Union[str, None] - etc.

        dtype = "STRING"
        mode = "REQUIRED"
        default = None
        description = None

        dtypes = get_args(field_annotation)

        if len(dtypes) == 0:
            if field_annotation in PYTHON_TYPES_TO_BIG_QUERY:
                dtype = PYTHON_TYPES_TO_BIG_QUERY[field_annotation]
        else:
            if type(None) in dtypes:
                mode = "NULLABLE"
            if dtypes[0] in PYTHON_TYPES_TO_BIG_QUERY:
                dtype = PYTHON_TYPES_TO_BIG_QUERY[dtypes[0]]

        return dtype, mode, default, description

    @field_validator("*", mode="before")
    @classmethod
    def remove_utc_from_datetime(cls, value: Any, info: ValidationInfo) -> Any:
        field_info = cls.model_fields[info.field_name]
        dtype, _, _, _ = cls.parse_field_info(field_info)
        if dtype == "DATETIME" and "UTC" in value:
            return value.split("UTC")[0].strip()
        return value