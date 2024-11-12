import requests
import json
import sys


# Taken from https://www.mydatahack.com/running-jobs-with-informatica-cloud-rest-api/
def get_session_id(username, password, logging):
    """Authenticate with username and password and
       retrieve icSessionId and serverUrl that are used for Subsequent API calls"""
    session_id = ''
    data = {'@type': 'login', 'username': username, 'password': password}
    url = "https://dm-us.informaticacloud.com/ma/api/v2/user/login"
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
    # We need to pass data in string instead of dict so that the data gets posted directly.
    r = requests.post(url, data=json.dumps(data), headers=headers)

    logging.info('\tAPI Login Response Status Code: ' + str(r.status_code))

    if r.status_code == 200:
        session_id = r.json()["icSessionId"]
        server_url = r.json()["serverUrl"]
        logging.info('\tSession Id: ' + session_id)
        logging.info('\tServer URL: ' + server_url)
    else:
        logging.info('API Login call failed:')
        logging.info(r.headers)
        logging.info(r.json())
        sys.exit(1)

    return session_id, server_url


def get_tasks(session_id, server_url, taskType, logging):
    """ Use this method to get a list of tasks of a specified type. This may be used to determine the TaskID of a task.
            Task Types: https://jsapi.apiary.io/apis/cloudrestapi/reference/job/list-of-tasks/login.html
                AVS-Contact validation task
                DMASK-Data masking task
                DQA-Data assessment task
                DRS-Data replication task
                DSS-Data synchronization task
                MTT-Mapping configuration task
                PCS-PowerCenter task"""
    task_list_url = server_url + "/api/v2/task?type=" + taskType
    headers = {'icSessionId': session_id}
    r = requests.get(task_list_url, headers=headers)

    if r.status_code == 200:
        logging.info('\tRetrieved list of all Tasks')
        response_dict = json.loads(r.content)
        return response_dict

    else:
        logging.info('\tFailed to get list of Tasks: ' + str(r.status_code))
        return {}


def get_task_id(response_dict, taskName, logging):
    for d in response_dict:
        if d['name'] == taskName:
            id = d['id']
            logging.info('\tTaskID: ' + id)
            return id

    logging.info('\tCould not find TaskID for the Task Name specified')
    return ""


def get_all_mapping_details(session_id, server_url, logging):
    mapping_details_url = server_url + "/api/v2/mapping"
    headers = {'icSessionId': session_id, 'HTTP': '1.0', 'Accept': 'application/json'}
    r = requests.get(mapping_details_url, headers=headers)

    if r.status_code == 200:
        response_dict = json.loads(r.content)
        return response_dict

    else:
        logging.info('\tFailed to get Mappings: ' + str(r.status_code))
        return {}


def get_singular_mapping_details(session_id, server_url, logging, mappingID):
    mapping_details_url = server_url + "/api/v2/mapping/" + mappingID
    headers = {'icSessionId': session_id, 'Accept': 'application/json'}
    r = requests.get(mapping_details_url, headers=headers)

    if r.status_code == 200:
        mapping_deets_dict = json.loads(r.content)
        return mapping_deets_dict

    else:
        logging.info('\tFailed to get Mapping details for mapping ' + mappingID + ': ' + str(r.status_code))
        return {}


def get_connection_details(session_id, server_url, logging):

    # source_dict = {}
    # target_dict = {}

    connections_url = server_url + "/api/v2/connection"
    # target_connections_url = server_url + "/api/v2/mapping"
    headers = {'icSessionId': session_id, 'content-type': 'application/json'}
    r = requests.get(connections_url, headers=headers)

    if r.status_code == 200:
        response_dict = json.loads(r.content)
        return response_dict

    else:
        logging.info('\tFailed to get Mappings: ' + str(r.status_code))
        return {}