"""Factory creating Airflow DAG."""

import os
from datetime import timedelta, datetime

import airflow
from airflow import DAG
from airflow.models import BaseOperator

from dbt_airflow_factory.ingestion import IngestionEngine, IngestionFactory

if airflow.__version__.startswith("1."):
    from airflow.operators.dummy_operator import DummyOperator
else:
    from airflow.operators.dummy import DummyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.models.dagrun import DagRun
from airflow.utils.state import DagRunState
from airflow.models.dagbag import DagBag

from typing import Dict, List

from pytimeparse import parse

from dbt_airflow_factory.builder_factory import DbtAirflowTasksBuilderFactory
from dbt_airflow_factory.config_utils import read_config, read_env_config
from dbt_airflow_factory.notifications.handler import NotificationHandlersFactory
from dbt_airflow_factory.tasks import ModelExecutionTasks
from dbt_airflow_factory.tasks_builder.builder import DbtAirflowTasksBuilder
from dbt_airflow_factory.tasks_builder.utils import generate_dag_id


class AirflowDagFactory:
    """
    Factory creating Airflow DAG.

    :param dag_path: path to ``manifest.json`` file.
    :type dag_path: str
    :param env: name of the environment.
    :type env: str
    :param dbt_config_file_name: name of the DBT config file.
        If not specified, default value is ``dbt.yml``.
    :type dbt_config_file_name: str
    :param execution_env_config_file_name: name of the execution env config file.
        If not specified, default value is ``execution_env.yml``.
    :type execution_env_config_file_name: str
    :param airflow_config_file_name: name of the Airflow config file.
        If not specified, default value is ``airflow.yml``.
    :type airflow_config_file_name: str
    """

    _builder: DbtAirflowTasksBuilder
    dag_path: str
    """path to ``manifest.json`` file."""
    env: str
    """name of the environment."""
    airflow_config_file_name: str
    """name of the Airflow config file (default: ``airflow.yml``)."""

    def __init__(
        self,
        dag_path: str,
        env: str,
        dbt_config_file_name: str = "dbt.yml",
        execution_env_config_file_name: str = "execution_env.yml",
        airflow_config_file_name: str = "airflow.yml",
        airbyte_config_file_name: str = "airbyte.yml",
        ingestion_config_file_name: str = "ingestion.yml",
    ):
        self._notifications_handlers_builder = NotificationHandlersFactory()
        self.airflow_config = self._read_config(dag_path, env, airflow_config_file_name)
        self._builder = DbtAirflowTasksBuilderFactory(
            dag_path,
            env,
            self.airflow_config,
            dbt_config_file_name,
            execution_env_config_file_name,
        ).create()
        self.dag_path = dag_path
        airbyte_config = read_env_config(
            dag_path=dag_path, env=env, file_name=airbyte_config_file_name
        )
        self.ingestion_config = read_env_config(
            dag_path=dag_path, env=env, file_name=ingestion_config_file_name
        )
        self.ingestion_tasks_builder_factory = IngestionFactory(
            ingestion_config=airbyte_config,
            name=IngestionEngine.value_of(
                self.ingestion_config.get("engine", IngestionEngine.AIRBYTE.value)
            ),
        )

    def create(self) -> List[DAG]:
        """
        Parse ``manifest.json`` and create tasks based on the data contained there.

        :return: Generated DAG.
        :rtype: airflow.models.dag.DAG
        """
        dags = []
        dags_config = (
            self.airflow_config["dag"]
            if type(self.airflow_config["dag"]) is list
            else [self.airflow_config["dag"]]
        )
        for dag_index, dag_properties in enumerate(dags_config):
            dag_properties["dagrun_timeout"] = (
                timedelta(**dag_properties["dagrun_timeout"])
                if "dagrun_timeout" in dag_properties
                else None
            )
            higher_priority_dag_ids = [
                higher_dags_properties["dag_id"]
                for higher_dags_properties in dags_config[0:dag_index]
            ]
            dag_properties["dag_id"] = generate_dag_id(dag_properties)
            with DAG(default_args=self.airflow_config["default_args"], **dag_properties) as dag:
                self.create_tasks(
                    dag_properties.get("schedule_interval"),
                    dag_properties.get("tags"),
                    higher_priority_dag_ids,
                )
                dags.append(dag)
        return dags

    def create_tasks(
        self, schedule: str = "", tags: List[str] = None, higher_priority_dag_ids: List[str] = None
    ) -> None:
        """
        Parse ``manifest.json`` and create tasks based on the data contained there.
        """

        ingestion_enabled = self.ingestion_config.get("enable", False)
        start = self._create_starting_task()

        if ingestion_enabled and self.ingestion_tasks_builder_factory:
            builder = self.ingestion_tasks_builder_factory.create()
            ingestion_tasks = builder.build()
            ingestion_tasks >> start

        if higher_priority_dag_ids:

            condition_check = self._create_condition_check(higher_priority_dag_ids)
            condition_check >> start
            if ingestion_enabled and self.ingestion_tasks_builder_factory:
                condition_check >> ingestion_tasks

        tasks = self._builder.parse_manifest_into_tasks(self._manifest_file_path(), tags, schedule)
        for starting_task in tasks.get_starting_tasks():
            start >> starting_task.get_start_task()
        self._add_ending_task_with_dependencies(tasks)

    def _add_ending_task_with_dependencies(self, tasks: ModelExecutionTasks) -> None:
        end = DummyOperator(task_id="end")
        if self.airflow_config.get("run_tests_last"):
            all_models_passed_task = DummyOperator(task_id="all_models_passed")
            for ending_task in tasks.get_ending_tasks():
                ending_task.get_start_task() >> all_models_passed_task
            for test_task in tasks.get_test_operators():
                all_models_passed_task >> test_task
                test_task >> end
        else:
            for ending_task in tasks.get_ending_tasks():
                ending_task.get_end_task() >> end

    def _create_starting_task(self) -> BaseOperator:
        if self.airflow_config.get("seed_task", True):
            return self._builder.create_seed_task()
        else:
            return DummyOperator(task_id="start")

    def _manifest_file_path(self) -> str:
        file_dir = self.airflow_config.get("manifest_dir_path", self.dag_path)
        return os.path.join(
            file_dir, self.airflow_config.get("manifest_file_name", "manifest.json")
        )

    def _read_config(self, dag_path: str, env: str, airflow_config_file_name: str) -> dict:
        """
        Read ``airflow.yml`` from ``config`` directory into a dictionary.

        :return: Dictionary representing ``airflow.yml``.
        :rtype: dict
        :raises KeyError: No ``default_args`` key in ``airflow.yml``.
        """
        config = read_config(dag_path, env, airflow_config_file_name, replace_jinja=True)
        if "retry_delay" in config["default_args"]:
            config["default_args"]["retry_delay"] = parse(config["default_args"]["retry_delay"])
        if "failure_handlers" in config:
            config["default_args"][
                "on_failure_callback"
            ] = self._notifications_handlers_builder.create_failure_handler(
                config["failure_handlers"]
            )
        return config

    def _create_condition_check(self, higher_priority_dag_ids: List[str]) -> BaseOperator:
        def check_if_any_dags_are_active(higher_priority_dag_ids: List[str]) -> bool:
            dag_runs_running = DagRun.find(higher_priority_dag_ids, state=DagRunState.RUNNING)
            dag_runs_queued = DagRun.find(higher_priority_dag_ids, state=DagRunState.QUEUED)
            dag_runs = dag_runs_running + dag_runs_queued
            print(dag_runs)

            return len(dag_runs) > 0

        def check_if_any_scheduled(higher_priority_dag_ids: List[str]) -> bool:
            time_difference = timedelta(minutes=15)
            bag = DagBag(collect_dags=False)
            if not bag:
                return False
            for dag_name in higher_priority_dag_ids:
                future_dag = bag.get_dag(dag_name)
                current_dagrun_info = future_dag.next_dagrun_info(None)

                print(current_dagrun_info.data_interval)
                next_dagrun_info = future_dag.next_dagrun_info(current_dagrun_info.data_interval)
                print(next_dagrun_info.data_interval)

                if (
                    next_dagrun_info.data_interval.end
                    < datetime.now(next_dagrun_info.data_interval.end.tzinfo) + time_difference
                ):
                    return True
            return False

        def check_condition():

            return not (
                check_if_any_dags_are_active(higher_priority_dag_ids)
                or check_if_any_scheduled(higher_priority_dag_ids)
            )

        condition_check = ShortCircuitOperator(
            task_id="condition_check", python_callable=check_condition
        )
        return condition_check
