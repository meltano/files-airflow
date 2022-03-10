from dags.dbt_generator_utilities import DbtGeneratorUtilities
import os
import yaml

MELTANO_PROJECT_ROOT = '/Users/pnadolny/Documents/Git/meltano_project/airflow_hack/airflow_hack'

dbt_utils = DbtGeneratorUtilities(
    'dag',
    MELTANO_PROJECT_ROOT,
    'MELTANO_ENVIRONMENT',
    'MELTANO_TARGET'
)

with open(os.path.join(MELTANO_PROJECT_ROOT, "orchestrate", "dag_definition.yml"), "r") as yaml_file:
    yaml_content = yaml.safe_load(yaml_file)
    dags = yaml_content.get("dags", {}).get("dag_definitions")

manifest = dbt_utils.load_manifest()
selections = {}
for dag_name, dag_def in dags.items():
    selection =  dbt_utils.get_models_from_select(
        'dev',
        dag_def["selection_rule"]
    )
    selection.remove("")
    selections[dag_name] = selection

cache = {
    "selections": selections,
    "manifest": manifest,
}
with open(os.path.join(MELTANO_PROJECT_ROOT, "orchestrate", "dbt_selection_cache.yml"), "w") as yaml_file:
    yaml.dump(cache, yaml_file, default_flow_style=False)
