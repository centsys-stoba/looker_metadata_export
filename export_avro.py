import looker_sdk
import fastavro

sdk = looker_sdk.init40()

def get_all_models():
    return sdk.all_lookml_models(fields="name, project_name, explores")

def write_models_to_avro(models, filename="out/model.avro"):
    models_list = []
    for model in models:
        models_list.append({
            'model_name': model.name or '', 
            'project_name': model.project_name or '', 
            'label': model.label or '',
        })
        
    schema = {
        "type": "record",
        "name": "Model",
        "fields": [
            {"name": "model_name", "type": "string"},
            {"name": "project_name", "type": "string"},
            {"name": "label", "type": "string"},
        ]
    }
    with open(filename, mode='wb') as file:
        fastavro.writer(file, schema, models_list)

def get_all_explores(models):
    explores_list = []
    for model in models:
        for explore in model.explores:
            if explore.hidden: continue
            explores_list.append({
                "model_name": model.name or "",
                "explore_name": explore.name or "",
                "description": explore.description or "",
                "label": explore.label or "",
                "group_label": explore.group_label or ""
            })
    return explores_list

def write_explores_to_avro(explores_list, filename="out/explore.avro"):
    schema = {
        "type": "record",
        "name": "Explore",
        "fields": [
            {"name": "model_name", "type": "string"},
            {"name": "explore_name", "type": "string"},
            {"name": "description", "type": "string"},
            {"name": "label", "type": "string"},
            {"name": "group_label", "type": "string"},
        ]
    }
    with open(filename, mode='wb') as file:
        fastavro.writer(file, schema, explores_list)

def get_all_fields(explores_list):
    dimensions_list = []
    measures_list = []
    for explore in explores_list:
        try:
            explore_detail = sdk.lookml_model_explore(lookml_model_name=explore["model_name"], explore_name=explore["explore_name"])
            for dimension in explore_detail.fields.dimensions:
                if dimension.hidden: continue
                dimensions_list.append({
                    "category": dimension.category or "",
                    "model_name": explore["model_name"] or "",
                    "explore_name": explore["explore_name"] or "",
                    "view_name": dimension.view or "",
                    "view_label": dimension.view_label or "",
                    "field_name": dimension.name or "",
                    "field_label": dimension.label or "",
                    "type": dimension.type or "",
                    "value_format": dimension.value_format or "",
                    "description": dimension.description or "",
                })
            for measure in explore_detail.fields.measures:
                if measure.hidden: continue
                measures_list.append({
                    "category": measure.category or "",
                    "model_name": explore["model_name"],
                    "explore_name": explore["explore_name"],
                    "view_name": measure.view,
                    "view_label": measure.view_label or "",
                    "field_name": measure.name,
                    "field_label": measure.label or "",
                    "type": measure.type or "",
                    "value_format": measure.value_format or "",
                    "description": measure.description or "",
                })
        except (looker_sdk.error.SDKError):
            # 404エラーの場合HTMLが返ってきてデコードエラーとなるため例外処理を行う
            print(f"エラーが発生したため{explore['model_name']}.{explore['explore_name']}の取得をスキップします")
    return dimensions_list + measures_list


def write_fields_to_avro(fields, filename="out/fields.avro"):
    schema = {
        "type": "record",
        "name": "View",
        "fields": [
            {"name": "model_name", "type": "string"},
            {"name": "explore_name", "type": "string"},
            {"name": "view_name", "type": "string"},
            {"name": "view_label", "type": "string"},
            {"name": "field_name", "type": "string"},
            {"name": "field_label", "type": "string"},
            {"name": "category", "type": "string"},
            {"name": "type", "type": "string"},
            {"name": "value_format", "type": "string"},
            {"name": "description", "type": "string"},
       ]
    }
    with open(filename, mode='wb') as file:
        fastavro.writer(file, schema, fields)

def main():
    models = get_all_models()
    write_models_to_avro(models)
    explores_list = get_all_explores(models)
    write_explores_to_avro(explores_list)
    fields = get_all_fields(explores_list)
    write_fields_to_avro(fields)

if __name__ == "__main__":
    main()