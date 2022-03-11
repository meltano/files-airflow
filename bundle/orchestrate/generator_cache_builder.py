import os
import yaml
import subprocess
import json
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


project_root = os.getenv("MELTANO_PROJECT_ROOT", os.getcwd())

meltano_bin = ".meltano/run/bin"

if not Path(project_root).joinpath(meltano_bin).exists():
    logger.warning(
        f"A symlink to the 'meltano' executable could not be found at '{meltano_bin}'. Falling back on expecting it to be in the PATH instead."
    )
    meltano_bin = "meltano"

with open(os.path.join(project_root, "orchestrate", "dag_definition.yml"), "r") as yaml_file:
    yaml_content = yaml.safe_load(yaml_file)
    dags = yaml_content.get("dags", {}).get("dag_definitions")


local_filepath = f"{project_root}/.meltano/transformers/dbt/target/manifest.json"
with open(local_filepath) as f:
    manifest = json.load(f)

def get_models_from_select(project_root, env, select_criteria):
    logger.info(f"Running dbt ls for {select_criteria}")
    output = subprocess.run(
        f"{meltano_bin} --environment={env} invoke dbt ls --select {select_criteria}".split(" "),
        cwd=project_root,
        check=True,
        capture_output=True,
    )
    return output.stdout.decode("utf-8").split("\n")

selections = {}
for dag_name, dag_def in dags.items():
    selection =  get_models_from_select(
        project_root,
        'dev',
        dag_def["selection_rule"]
    )
    selection.remove("")
    selections[dag_name] = selection

# Add Meltano schedules to cache
logger.info(f"Running Meltano schedule list..")
result = subprocess.run(
    [meltano_bin, "schedule", "list", "--format=json"],
    cwd=project_root,
    stdout=subprocess.PIPE,
    universal_newlines=True,
    check=True,
)
schedules = json.loads(result.stdout)

cache = {
    "selections": selections,
    "manifest": manifest,
    "meltano_schedules": schedules,
}
with open(os.path.join(project_root, "orchestrate", "generator_cache.yml"), "w") as yaml_file:
    yaml.dump(cache, yaml_file, default_flow_style=False)
