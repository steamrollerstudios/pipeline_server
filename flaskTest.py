from flask import Flask, request, Response, render_template
import json
import os
import random
import subprocess
from enum import Enum, IntEnum
from steamroller.plasticAPI.workspace import Workspace

app = Flask(__name__)

runningWorkflows = dict()

class StatusCode(IntEnum):
    ALREADY_COMPLETE = 2
    SUCCEEDED = 1
    RUNNING = 0
    UNTRACKED = -1
    FAILED = -2

class ConstantPaths(str, Enum):
    LUIGI_TASK_PATH = r"D:\SteamrollerDev\developerpackages\steamroller\steamroller.tools.publisher\python\steamroller\tools\publisher"

def getStatus(key):
    return runningWorkflows[key]['process'].poll() if key in runningWorkflows else -1

@app.route('/')
def taskUI():
    return render_template('index.html')

@app.route('/test')
def indexTest():
    # This is just a test page to allow me to send arbitrary requests to the actual publish route
    data = {
        "mayaFile": "serrano_flask_publish.ma",
        "workspacePath": "D:\\steamroller.pipelinetesting\\test\\modelPublish",
        "repo": "D:\\steamroller.pipelinetesting",
        "assetName": "Serrano",
        "publishNotes": "Some things have changed submitting on Flask; see below!\n• Some geo combining and renaming per Surfacing requests!\n• Still waiting on notes from Christian/Rigging team!\n• Tech checked & cleaned file",
        "userId": 4994,
        "taskId": 68670,
        "checkInComment": "Testing full publish pipeline from JSON data"
    }
    output = '<form method="post" action="/api/publishModel">'
    
    for key, val in data.items():
        if key == 'publishNotes':
            output += '<textarea name="{}">{}</textarea>'.format(key, val)
        else:
            output += '<input type="{}" name="{}" value="{}">'.format('text' if isinstance(val, str) else 'number', key, val)
    output += '<button>Submit</button></form>'
    return output

@app.route('/api/getTrackedJobs')
def getTrackedJobs():
    returnDict = dict()
    for (key, workflow) in runningWorkflows.items():
        returnDict[key] = workflow.copy()
        status = getStatus(key)
        print(key, status)
        if status is None:
            returnDict[key]['status'] = StatusCode.RUNNING
        elif status != 0:
            returnDict[key]['status'] = StatusCode.FAILED
        else:
            returnDict[key]['status'] = StatusCode.SUCCEEDED
        returnDict[key].pop('process', -1)

    return Response(json.dumps(returnDict), mimetype='text/json')

@app.route('/api/publishModel', methods=['POST'])
def runModelPublish():
    jobId = random.randint(1, 99999)
    jobFile = os.path.join(ConstantPaths.LUIGI_TASK_PATH, 'model_publish_{}.json'.format(jobId))
    while os.path.exists(jobFile):
        jobId = random.randint(1, 99999)
        jobFile = os.path.join(ConstantPaths.LUIGI_TASK_PATH, 'model_publish_{}.json'.format(jobId))
    with open(jobFile, 'w') as f:
        f.write(json.dumps(request.form))

    triggerModelPublish(jobId, request.form['repo']) # Async

    ret = {
        'id': jobId,
        'type': 'model_publish',
        'status': StatusCode.RUNNING,
        'links': {
            'get_status': '/api/jobStatus/model/{}'.format(jobId),
            'kill_job': '/api/killJob/model/{}'.format(jobId)
        }
    }

    return Response(json.dumps(ret), mimetype='text/json')

@app.route('/api/jobStatus/<string:type>/<int:jobId>')
def jobStatus(type, jobId):
    ret = {
        'id': jobId,
        'type': type + '_publish',
        'status': StatusCode.RUNNING,
        'links': {
            'get_status': '/api/jobStatus/{}/{}'.format(type, jobId),
            'kill_job': '/api/killJob/{}/{}'.format(type, jobId)
        }
    }
    key = "{}_{}".format(type, jobId)
    if key not in runningWorkflows:
        ret['status'] = StatusCode.UNTRACKED
    else:
        status = getStatus(key)
        if status is None:
            ret['status'] = StatusCode.RUNNING
        elif status != 0:
            ret['status'] = StatusCode.FAILED
        else:
            ret['status'] = StatusCode.SUCCEEDED
    return Response(json.dumps(ret), mimetype='text/json')

@app.route('/api/cleanJobs/<string:type>', defaults={'forcekill': 0})
@app.route('/api/cleanJobs/<string:type>/<int:forcekill>')
def cleanJobs(type, forcekill):
    firstSize = len(runningWorkflows)
    killed = 0
    for (key, workflow) in runningWorkflows.copy().items():
        if not key.startswith(type + '_') or not key in runningWorkflows:
            continue
        status = getStatus(key)
        if forcekill and status is None:
            killed += 1
            workflow['process'].terminate()
            runningWorkflows.pop(key, -1)
        if status is not None:
            runningWorkflows.pop(key, -1)
    return Response(json.dumps({ 'jobs_killed': killed, 'jobs_cleaned': firstSize - len(runningWorkflows) }), mimetype='text/json')

@app.route('/api/killJob/<string:type>/<int:jobId>')
def killJob(type, jobId):
    key = "{}_{}".format(type, jobId)
    ret = {
        'id': jobId,
        'type': type + '_publish',
        'status': StatusCode.RUNNING,
        'links': {
            'get_status': '/api/jobStatus/{}/{}'.format(type, jobId),
            'kill_job': '/api/killJob/{}/{}'.format(type, jobId)
        }
    }
    if key not in runningWorkflows:
        ret['status'] = StatusCode.UNTRACKED
    else:
        status = getStatus(key)
        if status is None:
            runningWorkflows[key]['process'].terminate()
            ret['status'] = StatusCode.SUCCEEDED
        elif status != 0:
            ret['status'] = StatusCode.FAILED
        else:
            ret['status'] = StatusCode.ALREADY_COMPLETE
    runningWorkflows.pop(key, -1)
    return Response(json.dumps(ret), mimetype='text/json')

def triggerModelPublish(jobId, repo):
    cwd = os.getcwd()
    os.chdir(repo)
    plastic = Workspace()
    plastic.update_workspace(undo_pending = True)
    os.chdir(ConstantPaths.LUIGI_TASK_PATH)
    # subprocess.run(['mayapy', os.path.abspath(os.path.join(ConstantPaths.LUIGI_TASK_PATH, 'runTasks.py')), str(jobId)], stderr=subprocess.STDOUT, check=True)
    proc = subprocess.Popen(['mayapy', os.path.abspath(os.path.join(ConstantPaths.LUIGI_TASK_PATH, 'runTasks.py')), str(jobId)], stderr=subprocess.STDOUT)
    runningWorkflows["{}_{}".format('model', jobId)] = {
        'process': proc,
        'jobId': jobId,
        'type': 'Model Publish'
    }
    os.chdir(cwd)