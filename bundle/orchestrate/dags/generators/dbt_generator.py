import logging
import yaml
from pendulum import datetime
from generators.base_generator import BaseGenerator
from airflow import DAG
try:
    from airflow.operators.bash_operator import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator


logger = logging.getLogger(__name__)


class DbtGenerator(BaseGenerator):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _make_dbt_task(self, dag, node, dbt_verb):
        """Returns an Airflow operator either run and test an individual model"""

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

    def _build_meltano_cmd(self, dbt_source_node, env, stream=False):
        parts = dbt_source_node.split(".")
        tap = parts[2]
        if stream:
            meltano_stream = parts[3]
            select_filter = f"--select {meltano_stream}"
        # TODO: this makes and assumption of how taps are named, we should add a translation layer for exceptional cases
        tap = tap.replace("_", "-")
        target = self.generator_configs("dbt", {}).get("default_target")
        return f"meltano --log-level=debug --environment={env} elt {tap} {target} {select_filter} --job_id={tap}_{target}"

    @staticmethod
    def _get_full_model_name(manifest, node):
        node_details = manifest["nodes"][node]
        path_sql = node_details["path"].replace("/", ".")
        path = path_sql.replace(".sql", "")
        package_name = node_details["package_name"]
        return f"{package_name}.{path}"

    def _build_tasks_list(self, dag, manifest, selected_models):
        dbt_tasks = {}
        for node in manifest["nodes"].keys():
            name = self._get_full_model_name(manifest, node)
            if node.split(".")[0] == "model" and name in selected_models:
                node_test = node.replace("model", "test")
                dbt_tasks[node] = self._make_dbt_task(dag, node, "run")
                dbt_tasks[node_test] = self._make_dbt_task(dag, node, "test")
        return dbt_tasks

    def _read_cache(self):
        local_filepath = f"{self.project_root}/orchestrate/generator_cache.yml"
        with open(local_filepath) as yaml_file:
            data = yaml.safe_load(yaml_file)
        return data

    def create_dag(self, dag_id, dag_def, args):
        return DAG(
            dag_id,
            catchup=False,
            default_args=args,
            schedule_interval=dag_def["interval"],
            # We don't care about start date since were not using it and its recommended
            # to be static so we just set it the same date for all
            start_date=datetime(2022, 1, 1),
            max_active_runs=1,
        )

    def create_tasks(self, dag, dag_def):

        cache = self._read_cache()
        manifest = cache.get("manifest")
        selected_models = cache.get("selections")
        dbt_tasks = self._build_tasks_list(dag, manifest, selected_models)

        for node in manifest["nodes"].keys():
            name = self._get_full_model_name(manifest, node)
            if node.split(".")[0] == "model" and name in selected_models:
                # Set dependency to run tests on a model after model runs finishes
                node_test = node.replace("model", "test")
                yield [dbt_tasks[node], dbt_tasks[node_test]]
                # Set all model -> model dependencies
                for upstream_node in manifest["nodes"][node]["depends_on"]["nodes"]:
                    upstream_node_type = upstream_node.split(".")[0]
                    if upstream_node_type == "model":
                        yield [dbt_tasks[upstream_node], dbt_tasks[node]]
                    elif upstream_node_type == "source":
                        # For source run Meltano jobs
                        meltano_cmd = self._build_meltano_cmd(upstream_node, self.env, stream=True)
                        meltano_task = BashOperator(
                            task_id=f"meltano-{upstream_node}",
                            bash_command=f"""
                            cd {self.project_root}; {meltano_cmd} 
                            """,
                            dag=dag,
                        )
                        yield [meltano_task, dbt_tasks[node]]
