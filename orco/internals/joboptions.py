class JobOptions:

    def __init__(self, timeout=None):
        self.timeout = timeout

    @staticmethod
    def parse_from_config(config):
        if isinstance(config, dict):
            job_dict = config.get("_job")
            if isinstance(job_dict, dict):
                timeout = job_dict.get("timeout")
                if timeout is not None \
                        and not isinstance(timeout, int) \
                        and not isinstance(timeout, float):
                    raise Exception("Timeout has to be a number")
                return JobOptions(timeout=timeout)
        return JobOptions()
