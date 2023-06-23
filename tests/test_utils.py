import pytest

from dbt_airflow_factory.tasks_builder.utils import transform_cron_expression


@pytest.mark.parametrize(
    "cron_expr, expected_result",
    [
        ("* * * * *", "x_x_x_x_x"),
        ("0 */2 * * *", "0_x/2_x_x_x"),
        ("0 0,12 * * *", "0_0-12_x_x_x"),
        ("*/5 * * * *", "x/5_x_x_x_x"),
        ("0 0 * * 0,6", "0_0_x_x_0-6"),
        ("0 0 * * 0-6", "0_0_x_x_0-6"),
        ("0 0 LW * *", "0_0_LW_x_x"),  # Last weekday of the month
        ("0 0 5W * SUN", "0_0_5W_x_SUN"),  # Nearest weekday to the 5th, only if Sunday
        ("0 0 1-10/2 * *", "0_0_1-10/2_x_x"),  # Range with step for day of the month
        ("0 0/30 8-17 * *", "0_0/30_8-17_x_x"),  # Range for hour, every 30 minutes
        ("0 0 1 JAN,APR,JUL,OCT *", "0_0_1_JAN-APR-JUL-OCT_x"),  # Specific months
        ("0 0 * * *", "0_0_x_x_x"),  # Every hour
        ("0 * * * *", "0_x_x_x_x"),  # Every minute
    ],
)
def test_transform_cron_expression(cron_expr, expected_result):
    assert transform_cron_expression(cron_expr) == expected_result
