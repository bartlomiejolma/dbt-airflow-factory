"""Class parsing ``manifest.json`` into Airflow tasks."""
import json
import logging
from typing import Any, ContextManager, Dict, Tuple

import airflow
from airflow.models.baseoperator import BaseOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.sensors.external_task_sensor import ExternalTaskSensor

from dbt_airflow_factory.tasks_builder.node_type import NodeType
from dbt_airflow_factory.tasks_builder.parameters import TasksBuildingParameters

if not airflow.__version__.startswith("1."):
    from airflow.utils.task_group import TaskGroup

from dbt_airflow_factory.operator import DbtRunOperatorBuilder, EphemeralOperator
from dbt_airflow_factory.tasks import ModelExecutionTask, ModelExecutionTasks
from dbt_airflow_factory.tasks_builder.gateway import TaskGraphConfiguration
from dbt_airflow_factory.tasks_builder.graph import DbtAirflowGraph
from dbt_airflow_factory.tasks_builder.utils import is_source_sensor_task


def branch_func_wrapper(skip_sensor: str, with_sensor: str):
    def branch_func(**kwargs):
        run_id = kwargs["run_id"]
        is_manual = run_id.startswith("manual__")
        if is_manual:
            return skip_sensor
        else:
            return with_sensor

    return branch_func


class DbtAirflowTasksBuilder:
    """
    Parses ``manifest.json`` into Airflow tasks.

    :param airflow_config: DBT node operator.
    :type airflow_config: TasksBuildingParameters
    :param operator_builder: DBT node operator.
    :type operator_builder: DbtRunOperatorBuilder
    :param gateway_config: DBT node operator.
    :type gateway_config: TaskGraphConfiguration
    """

    def __init__(
        self,
        airflow_config: TasksBuildingParameters,
        operator_builder: DbtRunOperatorBuilder,
        gateway_config: TaskGraphConfiguration,
    ):
        self.operator_builder = operator_builder
        self.airflow_config = airflow_config
        self.gateway_config = gateway_config

    def parse_manifest_into_tasks(self, manifest_path: str) -> ModelExecutionTasks:
        """
        Parse ``manifest.json`` into tasks.

        :param manifest_path: Path to ``manifest.json``.
        :type manifest_path: str
        :return: Dictionary of tasks created from ``manifest.json`` parsing.
        :rtype: ModelExecutionTasks
        """
        return self._make_dbt_tasks(manifest_path)

    def create_seed_task(self) -> BaseOperator:
        """
        Create ``dbt_seed`` task.

        :return: Operator for ``dbt_seed`` task.
        :rtype: BaseOperator
        """
        return self.operator_builder.create("dbt_seed", "seed")

    @staticmethod
    def _load_dbt_manifest(manifest_path: str) -> dict:
        with open(manifest_path, "r") as f:
            manifest_content = json.load(f)
            logging.debug("Manifest content: " + str(manifest_content))
            return manifest_content

    def _make_dbt_test_task(self, model_name: str, is_in_task_group: bool) -> BaseOperator:
        command = "test"
        return self.operator_builder.create(
            self._build_task_name(model_name, command, is_in_task_group),
            command,
            model_name,
            additional_dbt_args=["--indirect-selection=cautious"],
        )

    def _make_dbt_multiple_deps_test_task(
        self, test_names: str, dependency_tuple_str: str
    ) -> BaseOperator:
        command = "test"
        return self.operator_builder.create(dependency_tuple_str, command, test_names)

    def _make_dbt_run_task(self, model_name: str, is_in_task_group: bool) -> BaseOperator:
        command = "run"
        return self.operator_builder.create(
            self._build_task_name(model_name, command, is_in_task_group),
            command,
            model_name,
        )

    def _make_dbt_source_sensor_task(self, source_name: str) -> BaseOperator:
        command = "source freshness"
        select_name = f"source:{source_name}"
        return self.operator_builder.create(
            self._build_task_name(source_name, command.replace(" ", "_")),
            command,
            select_name,
        )

    @staticmethod
    def _build_task_name(model_name: str, command: str, is_in_task_group: bool) -> str:
        return command if is_in_task_group else f"{model_name}_{command}"

    @staticmethod
    def _create_task_group_for_model(
        model_name: str, use_task_group: bool
    ) -> Tuple[Any, ContextManager]:
        import contextlib

        is_first_version = airflow.__version__.startswith("1.")
        task_group = (
            None if (is_first_version or not use_task_group) else TaskGroup(group_id=model_name)
        )
        task_group_ctx = task_group or contextlib.nullcontext()
        return task_group, task_group_ctx

    def _create_task_for_model(
        self,
        model_name: str,
        is_ephemeral_task: bool,
        use_task_group: bool,
    ) -> ModelExecutionTask:
        if is_ephemeral_task:
            return ModelExecutionTask(EphemeralOperator(task_id=f"{model_name}__ephemeral"), None)

        (task_group, task_group_ctx) = self._create_task_group_for_model(model_name, use_task_group)
        is_in_task_group = task_group is not None
        with task_group_ctx:
            run_task = self._make_dbt_run_task(model_name, is_in_task_group)
            test_task = self._make_dbt_test_task(model_name, is_in_task_group)
            if not self.airflow_config.run_tests_last:
                # noinspection PyStatementEffect
                run_task >> test_task
        return ModelExecutionTask(run_task, test_task, task_group)

    def _create_task_from_graph_node(
        self,
        node_name: str,
        node: Dict[str, Any],
    ) -> ModelExecutionTask:
        if node["node_type"] == NodeType.MULTIPLE_DEPS_TEST:
            return self._multiple_deps_test_model_execution_task(node_name, node)
        elif node["node_type"] == NodeType.SOURCE_SENSOR:
            return self._create_dag_sensor(node)
        elif node["node_type"] == NodeType.MOCK_GATEWAY:
            return self._create_dummy_task(node)
        else:
            return self._create_task_for_model(
                node["select"],
                node["node_type"] == NodeType.EPHEMERAL,
                self.airflow_config.use_task_group,
            )

    def _multiple_deps_test_model_execution_task(self, node_name: str, node: Dict[str, Any]):
        if self.airflow_config.run_tests_last:
            return ModelExecutionTask(
                None,
                self._make_dbt_multiple_deps_test_task(node["select"], node_name),
            )
        else:
            return ModelExecutionTask(
                self._make_dbt_multiple_deps_test_task(node["select"], node_name),
                None,
            )

    def _create_tasks_from_graph(self, dbt_airflow_graph: DbtAirflowGraph) -> ModelExecutionTasks:
        result_tasks = {
            node_name: self._create_task_from_graph_node(node_name, node)
            for node_name, node in dbt_airflow_graph.graph.nodes(data=True)
        }
        for node, neighbour in dbt_airflow_graph.graph.edges():
            # noinspection PyStatementEffect
            if self.airflow_config.run_tests_last:
                if result_tasks[neighbour].get_start_task():
                    (
                        result_tasks[node].get_start_task()
                        >> result_tasks[neighbour].get_start_task()
                    )
            else:
                (result_tasks[node].get_end_task() >> result_tasks[neighbour].get_start_task())

        source_names = [
            source_name
            for source_name in dbt_airflow_graph.get_graph_sources()
            if "test" != source_name.split("_")[-1] and not is_source_sensor_task(source_name)
        ]
        sink_names = [
            sink_name
            for sink_name in dbt_airflow_graph.get_graph_sinks()
            if "test" != sink_name.split("_")[-1]
        ]
        return ModelExecutionTasks(
            result_tasks,
            source_names,
            sink_names,
        )

    def _make_dbt_tasks(self, manifest_path: str) -> ModelExecutionTasks:
        manifest = self._load_dbt_manifest(manifest_path)
        dbt_airflow_graph = self._create_tasks_graph(manifest)
        tasks_with_context = self._create_tasks_from_graph(dbt_airflow_graph)
        logging.info(f"Created {str(tasks_with_context.length())} tasks groups")
        return tasks_with_context

    def _create_tasks_graph(self, manifest: dict) -> DbtAirflowGraph:

        dbt_airflow_graph = DbtAirflowGraph(self.gateway_config)
        dbt_airflow_graph.add_execution_tasks(manifest)
        if self.airflow_config.enable_dags_dependencies:
            dbt_airflow_graph.add_external_dependencies(manifest)
        dbt_airflow_graph.create_edges_from_dependencies(
            self.airflow_config.enable_dags_dependencies, not self.airflow_config.run_tests_last
        )
        if not self.airflow_config.show_ephemeral_models:
            dbt_airflow_graph.remove_ephemeral_nodes_from_graph()
        dbt_airflow_graph.contract_test_nodes()
        return dbt_airflow_graph

    def _create_dag_sensor(self, node: Dict[str, Any]) -> ModelExecutionTask:

        skip_sensor_task_id = f"skip_sensor_{node['select']}"
        with_sensor_task_id = f"with_sensor_{node['select']}"

        branch_op = BranchPythonOperator(
            task_id=f"branch_task_{node['select']}",
            provide_context=True,
            python_callable=branch_func_wrapper(skip_sensor_task_id, with_sensor_task_id),
        )
        sensor = ExternalTaskSensor(
            task_id=with_sensor_task_id,
            external_dag_id=node["dag"],
            external_task_id=node["select"],
            timeout=24 * 60 * 60,
            allowed_states=["success"],
            failed_states=["failed", "skipped"],
            mode="reschedule",
            check_existence=True,
        )
        skip_sensor = DummyOperator(
            task_id=skip_sensor_task_id,
        )
        join_task_id = f"join_{node['select']}"

        join = self._make_dbt_source_sensor_task(node["select"])

        DummyOperator(
            task_id=join_task_id,
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )
        branch_op >> sensor >> join
        branch_op >> skip_sensor >> join
        # todo move parameters to configuration
        return ModelExecutionTask(join)

    @staticmethod
    def _create_dummy_task(node: Dict[str, Any]) -> ModelExecutionTask:
        return ModelExecutionTask(DummyOperator(task_id=node["select"]))
