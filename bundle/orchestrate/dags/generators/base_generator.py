from abc import ABC, abstractmethod
from airflow import DAG
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class BaseGenerator(ABC):

    def __init__(self, project_root, env, generator_configs):
        self.project_root = project_root
        self.env = env
        self.generator_configs = generator_configs

        meltano_bin = ".meltano/run/bin"

        if not Path(self.project_root).joinpath(meltano_bin).exists():
            logger.warning(
                f"A symlink to the 'meltano' executable could not be found at '{meltano_bin}'. Falling back on expecting it to be in the PATH instead."
            )
            meltano_bin = "meltano"
        self.meltano_bin = meltano_bin

    @abstractmethod
    def create_tasks(self, dag: DAG, dag_name: str, dag_def: dict):
        pass

    @abstractmethod
    def create_dag(self, dag_name: str, dag_def: dict, args: dict) -> DAG:
        pass