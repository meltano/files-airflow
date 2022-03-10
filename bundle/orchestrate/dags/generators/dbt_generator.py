import json
import logging
import subprocess
import yaml

logger = logging.getLogger(__name__)


class DbtGenerator:

    def __init__(self, project_root, env, target):
        self.project_root = project_root
        self.env = env
        self.target = target

    def load_manifest(self):
        local_filepath = f"{self.project_root}/.meltano/transformers/dbt/target/manifest.json"
        with open(local_filepath) as f:
            data = json.load(f)
        return data

    def make_dbt_task(self, dag, node, dbt_verb):
        """Returns an Airflow operator either run and test an individual model"""
        from airflow.operators.bash import BashOperator

        model = node.split(".")[-1]
        if dbt_verb == "run":
            dbt_task = BashOperator(
                task_id=node,
                bash_command=f"""
                cd {self.project_root}; meltano --environment={self.env} invoke dbt:{dbt_verb} --models {model}
                """,
                dag=dag,
            )
        elif dbt_verb == "test":
            node_test = node.replace("model", "test")
            dbt_task = BashOperator(
                task_id=node_test,
                bash_command=f"""
                cd {self.project_root}; meltano --environment={self.env} invoke dbt:{dbt_verb} --models {model}
                """,
                dag=dag,
            )
        return dbt_task

    def build_meltano_cmd(self, dbt_source_node, env, stream=False):
        parts = dbt_source_node.split(".")
        tap = parts[2]
        if stream:
            meltano_stream = parts[3]
            select_filter = f"--select {meltano_stream}"
        # TODO: this makes and assumption of how taps are named, we should add a translation layer for exceptional cases
        tap = tap.replace("_", "-")
        return f"meltano --log-level=debug --environment={env} elt {tap} {self.target} {select_filter} --job_id={tap}_{self.target}"

    @staticmethod
    def get_models_from_select(env, select_criteria):
        # TODO: this wont run on airflow container without Meltano installed, cache the dep tree
        cmd = f"meltano --environment={env} invoke dbt ls --select {select_criteria}"
        output = subprocess.run(cmd.split(" "), check=True, capture_output=True)
        return output.stdout.decode("utf-8").split("\n")

    @staticmethod
    def get_full_model_name(manifest, node):
        node_details = manifest["nodes"][node]
        path_sql = node_details["path"].replace("/", ".")
        path = path_sql.replace(".sql", "")
        package_name = node_details["package_name"]
        return f"{package_name}.{path}"

    def build_tasks_list(self, dag, manifest, selected_models):
        dbt_tasks = {}
        for node in manifest["nodes"].keys():
            name = self.get_full_model_name(manifest, node)
            if node.split(".")[0] == "model" and name in selected_models:
                node_test = node.replace("model", "test")
                dbt_tasks[node] = self.make_dbt_task(dag, node, "run")
                dbt_tasks[node_test] = self.make_dbt_task(dag, node, "test")
        return dbt_tasks

    def read_cache(self):
        local_filepath = f"{self.project_root}/orchestrate/dbt_selection_cache.yml"
        with open(local_filepath) as yaml_file:
            data = yaml.safe_load(yaml_file)
        return data

    def build_dag(self, dag):
        from airflow.operators.bash import BashOperator

        cache = self.read_cache()
        manifest = cache.get("manifest")
        selected_models = cache.get("selections")
        dbt_tasks = self.build_tasks_list(dag, manifest, selected_models)

        for node in manifest["nodes"].keys():
            name = self.get_full_model_name(manifest, node)
            if node.split(".")[0] == "model" and name in selected_models:
                # Set dependency to run tests on a model after model runs finishes
                node_test = node.replace("model", "test")
                yield dbt_tasks[node], dbt_tasks[node_test]
                # Set all model -> model dependencies
                for upstream_node in manifest["nodes"][node]["depends_on"]["nodes"]:
                    upstream_node_type = upstream_node.split(".")[0]
                    if upstream_node_type == "model":
                        yield dbt_tasks[upstream_node], dbt_tasks[node]
                    elif upstream_node_type == "source":
                        # For source run Meltano jobs
                        meltano_cmd = self.build_meltano_cmd(upstream_node, self.env, stream=True)
                        meltano_task = BashOperator(
                            task_id=f"meltano-{upstream_node}",
                            bash_command=f"""
                            cd {self.project_root}; {meltano_cmd} 
                            """,
                            dag=dag,
                        )
                        yield meltano_task, dbt_tasks[node]