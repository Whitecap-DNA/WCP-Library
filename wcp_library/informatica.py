import logging
import json
import time
from datetime import datetime
from typing import Optional

import requests
from yarl import URL

from wcp_library.time import convert_tz

logger = logging.getLogger(__name__)


class InformaticaError(Exception):
    pass


class InformaticaSession:
    def __init__(self, username: str, password: str):
        self.username: str = username
        self.password: str = password
        self._session_id: Optional[str] = None
        self._server_url: Optional[URL] = None

        self._get_session_id()

    def _get_session_id(self) -> None:
        """
        Authenticate with username and password

        :return: icSessionId, serverUrl
        """

        data = {'@type': 'login', 'username': self.username, 'password': self.password}
        url = "https://dm-us.informaticacloud.com/ma/api/v2/user/login"
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        response = requests.post(url, data=json.dumps(data), headers=headers)

        logger.debug('\tInformatica API Login Response Status Code: ' + str(response.status_code))

        if response.status_code == 200:
            logger.info('\tInformatica API Login Successful')
            self._session_id = response.json()["icSessionId"]
            self._server_url = URL(response.json()["serverUrl"])
        else:
            raise InformaticaError(f'\tInformatica API Login call failed: {response.status_code}')

    def get_tasks(self, task_type: str) -> dict:
        """
        Use this method to get a list of tasks of a specified type. This may be used to determine the TaskID of a task.
        Task Types: https://jsapi.apiary.io/apis/cloudrestapi/reference/job/list-of-tasks/login.html
            AVS-Contact validation task
            DMASK-Data masking task
            DQA-Data assessment task
            DRS-Data replication task
            DSS-Data synchronization task
            MTT-Mapping configuration task
            PCS-PowerCenter task

        :param taskType: Task Type
        :return: Task List
        """

        task_list_url = self._server_url / "api/v2/task"
        headers = {'icSessionId': self._session_id}
        response = requests.get(str(task_list_url), headers=headers, params={'type': task_type})

        logger.debug('\tRetrieved list of all Tasks')

        if response.status_code == 200:
            return json.loads(response.content)
        else:
            raise InformaticaError(f'\tFailed to get list of Tasks: {response.status_code}')

    def get_task_id(self, task_name: str, task_type: str) -> str:
        """
        Use this method to get the TaskID of a specified task. This may be used to run a task.

        :param task_name: Task Name
        :param task_type: Task Type
        :return: Task ID
        """

        tasks = self.get_tasks(task_type)
        for task in tasks:
            if task['name'] == task_name:
                return task['id']

        raise InformaticaError(f'\tFailed to find TaskID for the Task Name specified: {task_name}')


    def is_task_running(self, task_id: str) -> tuple[bool, datetime]:
        """
        Use this method to determine if a task is currently running.

        :param task_id: Task ID
        :return: Tuple of running status and startTime
        """

        task_status_url = self._server_url / f"api/v2/task/{task_id}/status"
        headers = {'icSessionId': self._session_id}
        response = requests.get(str(task_status_url), headers=headers)

        logger.debug(f'\tRetrieved status of Task {task_id}')

        if response.status_code == 200:
            task_status = json.loads(response.content)
            utc_time = datetime.strptime(task_status['startTimeUTC'], '%Y-%m-%dT%H:%M:%S.%fZ')
            local_time = convert_tz(utc_time, 'UTC')
            return task_status['status'] == 'RUNNING', local_time
        else:
            raise InformaticaError(f'\tFailed to get status of Task: {response.status_code}')

    def run_job(self, task_id: str, task_type: str) -> str:
        """
        Use this method to run a task.

        :param task_id: Task ID
        :param task_type: Task Type
        :return: Run ID
        """

        job_start_url = self._server_url / "api/v2/job"
        headers = {'Content-Type': 'application/json', 'icSessionId': self._session_id, 'Accept': 'application/json'}
        data = {'@type': 'job', 'taskId': task_id, 'taskType': task_type}
        response = requests.post(str(job_start_url), data=json.dumps(data), headers=headers)

        if response.status_code == 200:
            logger.info('Starting Informatica Job...')
            response_dict = json.loads(response.content)
            runID = response_dict['runId']
            return runID

        else:
            raise InformaticaError(f"Failed to start Informatica Job: {response.status_code}")

    def wait_until_job_finish(self, run_id: str) -> datetime:
        """
        Use this method to wait until a job finishes running.

        :param run_id:
        :return: End Time
        """

        job_status_url = self._server_url / f"api/v2/activity/activityLog"
        headers = {'icSessionId': self._session_id}

        while True:
            response = requests.get(str(job_status_url), headers=headers, params={'runId': run_id})
            if response.status_code == 200:
                response_dict = json.loads(response.content)

                if not response_dict['endTimeUtc']:
                    time.sleep(30)
                    continue
                if response_dict['state'] == 1:
                    logger.info('\tJob completed successfully')
                elif response_dict['state'] == 2:
                    logger.info('\tJob completed with errors')
                elif response_dict['state'] == 3:
                    raise InformaticaError('Job failed')
                return convert_tz(datetime.strptime(response_dict['endTimeUtc'], '%Y-%m-%dT%H:%M:%S.%fZ'), 'UTC')
            else:
                raise InformaticaError(f"Failed to get job status: {response.status_code}")

    def get_connection_details(self) -> dict:
        """
        Use this method to get a list of connections.

        :return: Connection List
        """

        connections_url = self._server_url / "api/v2/connection"
        headers = {'icSessionId': self._session_id, 'Accept': 'application/json'}
        response = requests.get(str(connections_url), headers=headers)

        logger.debug('\tRetrieved list of all Connections')

        if response.status_code == 200:
            return json.loads(response.content)
        else:
            raise InformaticaError(f'\tFailed to get list of Connections: {response.status_code}')

    def get_mapping_details(self, mapping_id: str) -> dict:
        """
        Use this method to get details of a specific mapping.

        :param mapping_id:
        :return:
        """

        mapping_details_url = self._server_url / f"api/v2/mapping/{mapping_id}"
        headers = {'icSessionId': self._session_id, 'Accept': 'application/json'}
        response = requests.get(str(mapping_details_url), headers=headers)

        logger.debug(f'\tRetrieved details of Mapping {mapping_id}')

        if response.status_code == 200:
            return json.loads(response.content)
        else:
            raise InformaticaError(f'\tFailed to get details of Mapping {mapping_id}: {response.status_code}')

    def get_all_mapping_details(self) -> dict:
        """
        Use this method to get details of all mappings.

        :return:
        """

        mapping_details_url = self._server_url / "api/v2/mapping"
        headers = {'icSessionId': self._session_id, 'Accept': 'application/json'}
        response = requests.get(str(mapping_details_url), headers=headers)

        logger.debug('\tRetrieved details of all Mappings')

        if response.status_code == 200:
            return json.loads(response.content)
        else:
            raise InformaticaError(f'\tFailed to get details of all Mappings: {response.status_code}')
