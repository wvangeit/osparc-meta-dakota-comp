import time
import json
import uuid
import logging
import pathlib as pl


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("ToolsMap")

POLLING_WAIT = 1  # second
DISABLE_UUID_CHECK_STRING = "DISABLE_UUID_CHECK"


class oSparcFileMap:
    def __init__(
        self,
        map_file_path: pl.Path,
        caller_file_path: pl.Path,
        polling_interval: float = POLLING_WAIT,
    ) -> None:
        logger.info("Creating caller map")
        self.uuid = str(uuid.uuid4())
        self.map_uuid = None
        logger.info(f"Optimizer uuid is {self.uuid}")

        self.polling_interval = polling_interval

        self.caller_file_path = caller_file_path
        if self.caller_file_path.exists():
            self.caller_file_path.unlink()
        self.map_file_path = map_file_path

    def create_map_input_payload(self, tasks_uuid, params_sets):
        payload = {}
        payload["uuid"] = tasks_uuid
        payload["caller_uuid"] = self.uuid
        payload["map_uuid"] = self.map_uuid
        payload["command"] = "run"
        payload["tasks"] = []

        for param_set in params_sets:
            task = {}
            task_input = task["input"] = {}
            task_output = task["output"] = {}

            task_input["InputFile1"] = {
                "type": "FileJSON",
                "filename": "input.json",
                "value": param_set,
            }
            task_output["OutputFile1"] = {
                "type": "FileJSON",
                "filename": "output.json",
            }

            payload["tasks"].append(task)

        return payload

    def read_map_output_payload(self, map_output_payload):
        tasks = map_output_payload["tasks"]

        objs_sets = []

        for task in tasks:
            if task["status"] != "SUCCESS":
                raise Exception(f"A task was not succesful: {task}")

            task_output = task["output"]

            objs_set = task_output["OutputFile1"]["value"]
            objs_sets.append(objs_set)

        return objs_sets

    def evaluate(self, params_set):
        logger.info(f"Evaluating: {params_set}")

        tasks_uuid = str(uuid.uuid4())
        map_input_payload = self.create_map_input_payload(tasks_uuid, params_set)

        self.caller_file_path.write_text(json.dumps(map_input_payload, indent=4))

        waiter = 0
        payload_uuid = ""
        while not self.map_file_path.exists() or payload_uuid != tasks_uuid:
            if self.map_file_path.exists():
                payload_uuid = json.loads(self.map_file_path.read_text())["uuid"]
                if payload_uuid == DISABLE_UUID_CHECK_STRING:
                    break
                if waiter % 10 == 0:
                    logger.info(
                        f"Waiting for tasks uuid to match: payload:{payload_uuid} tasks:{tasks_uuid}"
                    )
            else:
                if waiter % 10 == 0:
                    logger.info(
                        f"Waiting for map results at: {self.map_file_path.resolve()}"
                    )
            time.sleep(self.polling_interval)
            waiter += 1

        map_output_payload = json.loads(self.map_file_path.read_text())

        objs_set = self.read_map_output_payload(map_output_payload)

        logger.info(f"Evaluation results: {objs_set}")

        return objs_set

    def map_function(self, *map_input):
        _ = map_input[0]
        params = map_input[1]

        return self.evaluate(params)

    def __del__(self):
        payload = {
            "command": "stop",
            "caller_uuid": self.uuid,
            "map_uuid": self.map_uuid,
        }

        self.caller_file_path.write_text(json.dumps(payload, indent=4))

        self.status = "stopping"
