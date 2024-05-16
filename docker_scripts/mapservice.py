import osparc_client.models.file
import osparc_client
import osparc
import pathos
import json
import os
import time
import contextlib
import tempfile
import zipfile

import pathlib as pl
import logging

logging.basicConfig(level=logging.INFO, format="[%(filename)s:%(lineno)d] %(message)s")
logger = logging.getLogger(__name__)

POLLING_INTERVAL = 1  # second


def start(input_path, output_path, template_id, n_of_workers=1):
    """Start"""

    pyrunner = MapRunner(
        input_path,
        output_path,
        template_id,
        n_of_workers,
        polling_interval=POLLING_INTERVAL,
    )

    try:
        pyrunner.setup()
        pyrunner.start()
        pyrunner.teardown()
    except Exception as err:  # pylint: disable=broad-except
        logger.error(f"{err} . Stopping %s", exc_info=True)


@contextlib.contextmanager
def create_study_job(template_id, job_inputs, studies_api):
    job = studies_api.create_study_job(
        study_id=template_id,
        job_inputs=job_inputs,
    )
    try:
        yield job
    finally:
        studies_api.delete_study_job(template_id, job.id)


class MapRunner:
    def __init__(
        self, input_path, output_path, template_id, n_of_workers=1, polling_interval=1
    ):
        """Constructor"""

        self.input_path = input_path  # path where osparc write all our input
        self.output_path = output_path  # path where osparc write all our input
        self.template_id = template_id
        self.n_of_workers = n_of_workers

        self.input_tasks_dir_path = self.input_path
        self.input_tasks_path = self.input_tasks_dir_path / "input_tasks.json"

        self.output_tasks_dir_path = self.output_path
        self.output_tasks_path = self.output_tasks_dir_path / "output_tasks.json"

        if self.output_tasks_path.exists():
            self.output_tasks_path.unlink()

        self.polling_interval = polling_interval

    def setup(self):
        """Setup the Python Runner"""
        self.osparc_cfg = osparc.Configuration(
            host="10.43.103.149.nip.io:8006",
            username=os.environ["OSPARC_API_KEY"],
            password=os.environ["OSPARC_API_SECRET"],
        )
        self.api_client = osparc.ApiClient(self.osparc_cfg)
        self.studies_api = osparc_client.StudiesApi(self.api_client)

    def start(self):
        """Start the Python Runner"""
        logger.info("Starting map ...")

        import getpass

        logger.info(f"User: {getpass.getuser()}, UID: {os.getuid()}")
        logger.info(f"Input path: {self.input_path.resolve()}")

        waiter = 0
        while not self.input_tasks_path.exists():
            if waiter % 10 == 0:
                logger.info(f"Waiting for input file at {self.input_tasks_path}...")
            time.sleep(self.polling_interval)
            waiter += 1

        last_tasks_uuid = ""
        waiter = 0
        while True:
            input_dict = json.loads(self.input_tasks_path.read_text())
            command = input_dict["command"]

            if command == "stop":
                break
            elif command == "run":
                tasks_uuid = input_dict["uuid"]

                if tasks_uuid == last_tasks_uuid:
                    if waiter % 10 == 0:
                        logger.info("Waiting for new tasks uuid")
                    time.sleep(self.polling_interval)
                    waiter += 1
                else:
                    input_tasks = input_dict["tasks"]
                    output_tasks = self.run_tasks(
                        tasks_uuid, input_tasks, self.n_of_workers
                    )
                    output_tasks_content = json.dumps(
                        {"uuid": tasks_uuid, "tasks": output_tasks}
                    )
                    self.output_tasks_path.write_text(output_tasks_content)
                    logger.info(f"Finished a set of tasks: {output_tasks_content}")
                    last_tasks_uuid = tasks_uuid
                    waiter = 0
            else:
                raise ValueError("Command unknown: {command}")

            time.sleep(self.polling_interval)

    def run_tasks(self, tasks_uuid, input_tasks, n_of_workers):
        logger.info(f"Evaluating: {input_tasks}")

        def map_func(task):
            logger.info(f"Running worker for task: {task}")

            input = task["input"]
            output = task["output"]

            job_inputs = {"values": {}}

            for param_name, param_input in input.items():
                param_type = param_input["type"]
                param_value = param_input["value"]
                if param_type == "FileJSON":
                    param_filename = param_input["filename"]
                    tmp_dir = tempfile.TemporaryDirectory()
                    tmp_dir_path = pl.Path(tmp_dir.name)
                    tmp_input_file_path = tmp_dir_path / param_filename
                    tmp_input_file_path.write_text(json.dumps(param_value))

                    input_data_file = osparc.FilesApi(self.api_client).upload_file(
                        file=tmp_input_file_path
                    )
                    job_inputs["values"][param_name] = input_data_file
                elif param_type == "file":
                    file_info = json.loads(param_value)
                    input_data_file = osparc_client.models.file.File(
                        id=file_info["id"],
                        filename=file_info["filename"],
                        content_type=file_info["content_type"],
                        checksum=file_info["checksum"],
                        e_tag=file_info["e_tag"],
                    )
                    job_inputs["values"][param_name] = input_data_file
                elif param_type == "integer":
                    job_inputs["values"][param_name] = int(param_value)
                elif param_type == "float":
                    job_inputs["values"][param_name] = float(param_value)
                else:
                    job_inputs["values"][param_name] = param_value

            logger.debug(f"Sending inputs: {job_inputs}")

            with create_study_job(
                self.template_id, job_inputs, self.studies_api
            ) as job:
                job_status = self.studies_api.start_study_job(
                    study_id=self.template_id, job_id=job.id
                )

                while job_status.state != "SUCCESS" and job_status.state != "FAILED":
                    job_status = self.studies_api.inspect_study_job(
                        study_id=self.template_id, job_id=job.id
                    )
                    time.sleep(1)

                task["status"] = job_status.state

                if job_status.state == "FAILED":
                    logger.error(f"Task failed: {task}")
                else:
                    results = self.studies_api.get_study_job_outputs(
                        study_id=self.template_id, job_id=job.id
                    ).results

                    for probe_name, probe_output in results.items():
                        if probe_name not in output:
                            raise ValueError(f"Unknown probe in output: {probe_name}")
                        probe_type = output[probe_name]["type"]

                        if probe_type == "FileJSON":
                            output_file = pl.Path(
                                osparc.FilesApi(self.api_client).download_file(
                                    probe_output.id
                                )
                            )
                            with zipfile.ZipFile(output_file, "r") as zip_file:
                                file_results_path = zipfile.Path(
                                    zip_file, at=output[probe_name]["filename"]
                                )
                                file_results = json.loads(file_results_path.read_text())

                            output[probe_name]["value"] = file_results
                        elif probe_type == "file":
                            tmp_output_data_file = osparc.FilesApi(
                                self.api_client
                            ).download_file(probe_output.id)
                            output_data_file = osparc.FilesApi(
                                self.api_client
                            ).upload_file(tmp_output_data_file)

                            output[probe_name]["value"] = json.dumps(
                                output_data_file.to_dict()
                            )
                        elif probe_type == "integer":
                            output[probe_name]["value"] = int(probe_output)
                        elif probe_type == "float":
                            output[probe_name]["value"] = float(probe_output)
                        else:
                            output[probe_name]["value"] = probe_output

                    logger.info(f"Worker has finished task: {task}")

            return task

        logger.info(f"Starting tasks on {n_of_workers} workers")
        with pathos.pools.ThreadPool(nodes=n_of_workers) as pool:
            output_tasks = list(pool.map(map_func, input_tasks))
            pool.close()
            pool.join()
            pool.clear()  # Pool is singleton, need to clear old pool

        return output_tasks

    def teardown(self):
        logger.info("Closing map ...")
        self.api_client.close()

    def read_keyvalues(self):
        """Read keyvalues file"""

        keyvalues_unprocessed = json.loads(self.keyvalues_path.read_text())
        self.keyvalues_path.unlink()

        keyvalues = {}
        for key, value in keyvalues_unprocessed.items():
            keyvalues[key] = {}
            keyvalues[key][value["key"]] = value["value"]

        return keyvalues
