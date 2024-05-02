from google.cloud import bigquery

def main(project: str,
         main_dataset: str,
         location: str,
         taxonomy_name: str,
         policy_tag_ids: dict,
         table_policy_tag: list,
         bigquery_client: bigquery.Client):
    
    taxonomy_id = policy_tag_ids[taxonomy_name]
    
    tables = {}
    header = table_policy_tag.pop(0)
    for row in table_policy_tag:
        dataset, table, column, policy_tag = row
        
        if main_dataset is not None and dataset != main_dataset:
            continue
        
        table_ref = f"{dataset}.{table}"
        if table_ref not in tables:
            tables[table_ref] = {}
        
        tables[table_ref][column.upper()] = policy_tag
        
    # Recorremos cada tabla en data_by_tables 
    for table_ref, table_info in tables.items():
        table_ref = f"{project}.{table_ref}"
        try:
            table = bigquery_client.get_table(table_ref)
        # Si no existe la tabla indicamos que no existe y continuamos con la siguiente.
        except Exception as error:
            error = f"Tabla {table_ref} no existe."
            print(error)
            continue
        
        # new_schema es el schema modificado con los policy tag que tendra la tabla
        new_schema = []
        
        # recorremos cada schema_field (columna) y lo actualizamos en los casos que corresponda
        try:
            for schema_field in table.schema:
                column = schema_field.name
                
                # si la columna dada tiene asociado un policy tag ...
                if column.upper() in table_info:
                    policy_tag_name = table_info[column.upper()]
                    policy_tag_id = policy_tag_ids[policy_tag_name]
                    policy_tag_resource_path = f"projects/{project}/locations/{location}/taxonomies/{taxonomy_id}/policyTags/{policy_tag_id}"
                    policy_tag = bigquery.PolicyTagList(names=(policy_tag_resource_path,))
                else:
                    # Esto deja el policy tag que tenia por defecto
                    policy_tag = None
                    
                # Esto elimina el policy tag de la columna.
                # policy_tag = bigquery.PolicyTagList(names=(None, ))
                new_schema_field = bigquery.SchemaField(name=schema_field.name,
                                                        field_type=schema_field.field_type,
                                                        mode=schema_field.mode,
                                                        description=schema_field.description,
                                                        fields=schema_field.fields,
                                                        precision=schema_field.precision,
                                                        scale=schema_field.scale,
                                                        max_length=schema_field.max_length,
                                                        default_value_expression=schema_field.default_value_expression,
                                                        policy_tags=policy_tag)
                
                # agregamos el nuevo schema_field al nuevo schema
                new_schema.append(new_schema_field)
                
            # actualizamos el schema de la tabla
            table.schema = new_schema
            table = bigquery_client.update_table(table, ["schema"])
            print(f"Se actualizo el schema de la tabla {table_ref}")
        except Exception as error:
            error = f"Ocurrio un error al actualizar el schema de {table_ref}\n{error}"
            print(error)