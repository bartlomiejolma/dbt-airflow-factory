class TasksBuildingParameters:
    def __init__(
        self,
        use_task_group: bool = True,
        show_ephemeral_models: bool = True,
        enable_dags_dependencies: bool = False,
        run_tests_last: bool = False,
    ) -> None:
        self.use_task_group = use_task_group
        self.show_ephemeral_models = show_ephemeral_models
        self.enable_dags_dependencies = enable_dags_dependencies
        self.run_tests_last = run_tests_last
