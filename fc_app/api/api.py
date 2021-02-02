import queue as q

import redis
import rq
from flask import Blueprint, jsonify, request, current_app

from fc_app.api.data_helper import receive_client_data, receive_coordinator_data, broadcast_data_to_clients, \
    send_data_to_coordinator, send_finished_flag_to_coordinator
from fc_app.api.status_helper import init, local_calculation, waiting, global_calculation, broadcast_results, \
    write_output, finalize, start, finished
from redis_util import redis_set, redis_get, get_step, set_step

pool = redis.BlockingConnectionPool(host='localhost', port=6379, db=0, queue_class=q.Queue)
r = redis.Redis(connection_pool=pool)

# setting 'available' to False --> no data will be send around.
# Change it to True later to send data from the coordinator to the clients or vice versa.
redis_set('available', False)

# The various steps of the app. This list is not really used and only an overview.
STEPS = ['start', 'init', 'local_calculation', 'waiting', 'global_calculation', 'broadcast_results', 'write_output',
         'finalize', 'finished']

# Initializes the app with the first step
set_step('start')
# Initialize the local and global data
redis_set('local_data', None)
redis_set('global_data', [])

api_bp = Blueprint('api', __name__)
tasks = rq.Queue('fc_tasks', connection=r)


@api_bp.route('/status', methods=['GET'])
def status():
    """
    GET request to /status, if True is returned a GET data request will be send
    :return: JSON with key 'available' and value True or False and 'finished' value True or False
    """
    available = redis_get('available')
    current_app.logger.info('[STATUS] GET request ' + str(available))

    switcher = {
        "start": start,
        "init": init,
        "local_calculation": local_calculation,
        "waiting": waiting,
        "global_calculation": global_calculation,
        "broadcast_results": broadcast_results,
        "write_output": write_output,
        "finalize": finalize,
        "finished": finished,
    }

    if get_step() != "finished":
        func = switcher.get(get_step(), lambda: "Invalid state")
        func()
        return jsonify({'available': True if available else False, 'finished': False})
    else:
        return jsonify({'available': False, 'finished': True})


@api_bp.route('/data', methods=['GET', 'POST'])
def data():
    """
    GET request to /data sends data to coordinator or clients
    POST request to /data pulls data from coordinator or clients
    :return: GET request: JSON with key 'data' and value data
             POST request: JSON True
    """
    if request.method == 'POST':
        current_app.logger.info('[DATA] POST request')
        if redis_get('is_coordinator'):
            receive_client_data(request.get_json(True))
        if not redis_get('is_coordinator'):
            receive_coordinator_data(request.get_json(True))
        return jsonify(True)

    elif request.method == 'GET':
        current_app.logger.info('[DATA] GET request')
        if redis_get('is_coordinator'):
            global_result = broadcast_data_to_clients()
            return jsonify({'global_result': global_result})
        if not redis_get('is_coordinator'):
            if get_step() != 'finalize':
                local_data = send_data_to_coordinator()
                return jsonify({'data': local_data})
            if get_step() == "finalize":
                local_finished_flag = send_finished_flag_to_coordinator()
                return jsonify({'finished': local_finished_flag})

    else:
        current_app.logger.info('[API] Wrong request type, only GET and POST allowed')
        return jsonify(True)


@api_bp.route('/setup', methods=['POST'])
def setup():
    """
    set setup params
    - id is the id of the client
    - coordinator is True if the client is the coordinator,
    - in global_data the data from all clients (including the coordinator) will be aggregated
    - clients is a list of all ids from all clients
    - nr_clients is the number of clients involved in the app
    :return: JSON True
    """
    current_app.logger.info('[STEP] setup')
    current_app.logger.info('[API] Retrieve Setup Parameters')
    setup_params = request.get_json()
    redis_set('id', setup_params['id'])
    is_coordinator = setup_params['master']
    redis_set('is_coordinator', is_coordinator)
    if is_coordinator:
        redis_set('global_data', [])
        redis_set('finished', [])
        redis_set('clients', setup_params['clients'])
        redis_set('nr_clients', len(setup_params['clients']))
    set_step('init')
    return jsonify(True)
