import csv
import looker_sdk

sdk = looker_sdk.init40()

def get_all_models():
    return sdk.all_lookml_models(fields="name, project_name, explores")

def write_models_to_csv(models, filename="out/models.csv"):
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["model_name", "project_name", "label"])
        for model in models:
            writer.writerow([model.name, model.project_name, model.label])

def get_all_explores(models):
    explores_list = []
    for model in models:
        for explore in model.explores:
            if explore.hidden: continue
            explores_list.append({
                "model_name": model.name,
                "explore_name": explore.name,
                "description": explore.description or "",
                "label": explore.label or "",
                "group_label": explore.group_label or ""
            })
    return explores_list

def write_explores_to_csv(explores_list, filename="out/explores.csv"):
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["model_name", "explore_name", "description", "label", "group_label"])
        for explore in explores_list:
            writer.writerow([explore["model_name"], explore["explore_name"], explore["description"], explore["label"], explore["group_label"]])

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
                    "model_name": explore["model_name"],
                    "explore_name": explore["explore_name"],
                    "view_name": dimension.view,
                    "view_label": dimension.view_label or "",
                    "field_name": dimension.name,
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

def write_fields_to_csv(fields, filename="out/fields.csv"):
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["model_name", 
                         "explore_name", 
                         "view_name", 
                         "view_label", 
                         "field_name", 
                         "field_label", 
                         "category",
                         "type", 
                         "value_format", 
                         "description", 
                         ])
        for field in fields:
            writer.writerow([field['model_name'], 
                             field['explore_name'], 
                             field['view_name'], 
                             field['view_label'], 
                             field['field_name'], 
                             field['field_label'], 
                             field['category'], 
                             field['type'], 
                             field['value_format'],
                             field['description'],
                             ])

def main():
    models = get_all_models()
    write_models_to_csv(models)
    explores_list = get_all_explores(models)
    write_explores_to_csv(explores_list)
    fields = get_all_fields(explores_list)
    write_fields_to_csv(fields)

if __name__ == "__main__":
    main()