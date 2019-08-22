class TaskOptions:

    def __init__(self, timeout=None):
        self.timeout = timeout

    @staticmethod
    def parse_from_config(config):
        if isinstance(config, dict):
            task_dict = config.get("_task")
            if isinstance(task_dict, dict):
                timeout = task_dict.get("timeout")
                if timeout is not None \
                        and not isinstance(timeout, int) \
                        and not isinstance(timeout, float):
                    raise Exception("Timeout has to be a number")
                return TaskOptions(timeout=timeout)
        return TaskOptions()
