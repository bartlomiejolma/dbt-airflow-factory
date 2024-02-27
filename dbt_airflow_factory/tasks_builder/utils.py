from typing import Dict, List, Optional


def is_task_type(node_name: str, task_type: str) -> bool:
    return node_name.split(".")[0] == task_type


def is_model_run_task(node_name: str) -> bool:
    return is_task_type(node_name, "model")


def is_snapshot_task(node_name: str) -> bool:
    return is_task_type(node_name, "snapshot")


def is_source_sensor_task(node_name: str) -> bool:
    return is_task_type(node_name, "source")


def is_test_task(node_name: str) -> bool:
    return is_task_type(node_name, "test")


def is_ephemeral_task(node: dict) -> bool:
    return node["config"]["materialized"] == "ephemeral"


def does_model_have_appropriate_tags(node: dict, tags: Optional[List[str]] = None) -> bool:
    if not tags:
        return True

    if set(tags).intersection(set(node["tags"])):
        return True
    return False


def transform_cron_expression(cron_expr: str) -> str:
    """
    This function accepts a string `cron_expr` and returns another string. The `cron_expr` string
    represents a cron schedule. It splits the `cron_expr` into components and transforms each
    component by replacing any `,` in it with `-`. It then joins the transformed components
    with `_` and replaces any `*` with `x`.

    Args:
        cron_expr (str): A string representing a cron schedule.

    Returns:
        str: A transformed string representing a cron schedule.

    """
    components = cron_expr.split()  # split the cron expression into its components
    transformed_components = []
    for component in components:
        # iterate through each component and transform it by replacing , with -
        transformed_component = "-".join(val for val in component.split(",") if val)
        transformed_components.append(transformed_component)
    # join all the transformed components with "_" and replace any "*" with "x"
    new_expr = "_".join(transformed_components).replace("*", "x")
    return new_expr


def generate_dag_id(properties: Dict) -> str:
    """
    This function takes a dictionary `properties` as input and returns a string
    that represents the `dag_id`.

    Args:
        properties (Dict): A dictionary containing the `dag_id` key and the `schedule_interval` key.

    Returns:
        str: A string representing the `dag_id`.

    """

    # apply the transform_cron_expression function on the `schedule_interval` key in `properties`
    schedule_interval = transform_cron_expression(properties["schedule_interval"])
    # create a list of `dag_id_parts` which consists of the `dag_id` key
    # in `properties` and the transformed `schedule_interval`
    dag_id_parts = [properties["dag_id"], schedule_interval]
    # join `dag_id_parts` with `_` and return the resulting string as the `dag_id`
    dag_id = "_".join(dag_id_parts)

    return dag_id
