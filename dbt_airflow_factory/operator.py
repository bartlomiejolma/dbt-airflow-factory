"""Factories creating Airflow Operators running DBT tasks."""

import abc
from typing import List, Optional

import airflow

from dbt_airflow_factory.dbt_parameters import DbtExecutionEnvironmentParameters
from dbt_airflow_factory.k8s_parameters import KubernetesExecutionParameters

if airflow.__version__.startswith("1."):
    from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
    from airflow.operators.dummy_operator import DummyOperator
else:
    from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
        KubernetesPodOperator,
    )
    from airflow.operators.dummy import DummyOperator

from airflow.models.baseoperator import BaseOperator


class DbtRunOperatorBuilder(metaclass=abc.ABCMeta):
    """
    Base class of a factory creating Airflow
    :class:`airflow.models.baseoperator.BaseOperator` running a single DBT task.
    """

    @abc.abstractmethod
    def create(self, name: str, command: str, model: Optional[str] = None) -> BaseOperator:
        """
        Create Airflow Operator running a single DBT task.

        :param name: task name.
        :type name: str
        :param command: DBT command to run.
        :type command: str
        :param model: models to include.
        :type model: Optional[str]
        :return: Airflow Operator running a single DBT task.
        :rtype: BaseOperator
        """
        raise NotImplementedError


class EphemeralOperator(DummyOperator):
    """
    :class:`DummyOperator` representing ephemeral DBT model.
    """

    ui_color = "#F3E4F7"
