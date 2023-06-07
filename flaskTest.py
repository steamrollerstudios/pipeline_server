from flask import Flask, request, Response, render_template
from flask_cors import CORS
import sys
import json
import os
import random
import subprocess
from enum import Enum, IntEnum
import socket

from steamroller.plasticAPI.workspace import Workspace

sys.path.insert(0, r'{}\Lib\site-packages'.format(os.environ['CONDA_PREFIX']))
import psycopg2
import psycopg2.extras
from psycopg2.extensions import AsIs

app = Flask(__name__)
CORS(app)

pipeline_db = psycopg2.connect(database="pipeline",
                        host="10.100.0.48",
                        user="steamroller",
                        password="!uQ7Br&wratR",
                        port="5432")

dbcursor = pipeline_db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

runningWorkflows = dict()

class StatusCode(IntEnum):
    ALREADY_COMPLETE = 2
    SUCCEEDED = 1
    RUNNING = 0
    UNTRACKED = -1
    FAILED = -2

class ConstantPaths(str, Enum):
    LUIGI_TASK_PATH = r"D:\SteamrollerDev\developerpackages\steamroller\steamroller.tools.publisher\python\steamroller\tools\publisher",
    PIPELINE_JOBS_TABLE = 'public."Pipeline_Jobs"'

HOSTNAME = socket.gethostname()
dbcursor.execute('SELECT * FROM {}'.format(ConstantPaths.PIPELINE_JOBS_TABLE))
PIPELINE_COLUMNS = tuple(col.name for col in dbcursor.description)

def deleteJobFromDb(type, jobId):
    dbcursor.execute("DELETE FROM {} WHERE jobid = {} AND jobtype = '{}'".format(ConstantPaths.PIPELINE_JOBS_TABLE, jobId, type))
    pipeline_db.commit()

def updateLocalWorkflowInfo():
    dbcursor.execute('SELECT * FROM {}'.format(ConstantPaths.PIPELINE_JOBS_TABLE))
    results = dbcursor.fetchall()
    for workflow in results:
        key = "{}_{}".format(workflow['jobtype'].strip(), workflow['jobid'])
        entry = runningWorkflows[key] if key in runningWorkflows else dict()
        runningWorkflows[key] = entry
        for column in workflow:
            entry[column] = workflow[column].strip() if isinstance(workflow[column], str) else workflow[column]
    pipeline_db.commit()

def ensureCorrectColumns(data):
    keys = data.keys()
    for key in keys:
        if not key in PIPELINE_COLUMNS:
            data.pop(key)
    return data

def getUpdatedDefaultWorkflow(type, jobId, data):
    result = {
        'jobid': jobId,
        'jobtype': type,
        'jobname': '{}_publish'.format(type),
        'jobstatus': 0,
        'machinename': HOSTNAME,
        'triggeredby': 'Anonymous',
        'taskstatus': 'Running...',
        'executorip': '0.0.0.0'
    }
    result.update(data)
    return result

def updateRemoteWorkflowInfo(type, jobId, data, includeDefaults = False):
    if includeDefaults:
        result = getUpdatedDefaultWorkflow(type, jobId, data)
    else:
        result = dict()
        result.update(data)
    result = ensureCorrectColumns(result)
    columns = result.keys()
    values = tuple(result[column] for column in columns)
    statement = "INSERT INTO {} (%s) VALUES %s".format(ConstantPaths.PIPELINE_JOBS_TABLE)
    prepared_statement = dbcursor.mogrify(statement, (AsIs(', '.join(columns)), values))
    dbcursor.execute(prepared_statement)
    pipeline_db.commit()

def getStatus(key):
    if key not in runningWorkflows:
        return -1
    elif 'process' not in runningWorkflows[key]:
        return runningWorkflows[key]['jobstatus']
    else:
        status = runningWorkflows[key]['process'].poll()
        return status

def getJobParamFilename(type, jobId):
    if type == 'model':
        return os.path.join(ConstantPaths.LUIGI_TASK_PATH, 'model_publish_{}.json'.format(jobId))
    return ''

def removeQuiet(fn):
    try:
        os.remove(fn)
    except:
        pass

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
    updateLocalWorkflowInfo()
    returnDict = dict()
    for (key, workflow) in runningWorkflows.items():
        returnDict[key] = workflow.copy()
        status = getStatus(key)
        if status is None:
            returnDict[key]['jobstatus'] = StatusCode.RUNNING
        elif status != 0:
            returnDict[key]['jobstatus'] = StatusCode.FAILED
        else:
            returnDict[key]['jobstatus'] = StatusCode.SUCCEEDED
        returnDict[key]['links'] = {
            'get_status': '/api/jobStatus/{}/{}'.format(returnDict[key]['jobtype'], returnDict[key]['jobid']),
            'kill_job': '/api/killJob/{}/{}'.format(returnDict[key]['jobtype'], returnDict[key]['jobid'])
        }
        returnDict[key].pop('process', -1)

    return Response(json.dumps(returnDict), mimetype='text/json')

@app.route('/api/restartJob/<string:type>/<int:jobId>')
def restartJob(type, jobId):
    jobFile = getJobParamFilename(type, jobId)
    if not os.path.exists(jobFile):
        return Response(json.dumps({'error': '-1'}), mimetype='text/json')
    
    with open(jobFile, 'r') as f:
        params = json.load(f)
    
    result = dict()
    if type == 'model':
        result = triggerModelPublish(jobId, params['repo'])

    return Response(json.dumps(result), mimetype='text/json')

@app.route('/api/publishModel', methods=['POST'])
def runModelPublish():
    jobId = random.randint(1, 99999)
    jobFile = getJobParamFilename('model', jobId)
    while os.path.exists(jobFile):
        jobId = random.randint(1, 99999)
        jobFile = getJobParamFilename('model', jobId)
    with open(jobFile, 'w') as f:
        f.write(json.dumps(request.form))

    ret = triggerModelPublish(jobId, request.form['repo']) # Async

    return Response(json.dumps(ret), mimetype='text/json')

@app.route('/api/jobStatus/<string:type>/<int:jobId>')
def jobStatus(type, jobId):
    ret = {
        'id': jobId,
        'type': type,
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
            removeQuiet(getJobParamFilename(type, workflow['jobid']))
            deleteJobFromDb(type, workflow['jobid'])
            runningWorkflows.pop(key, -1)
        elif status is not None:
            removeQuiet(getJobParamFilename(type, workflow['jobid']))
            deleteJobFromDb(type, workflow['jobid'])
            runningWorkflows.pop(key, -1)
    return Response(json.dumps({ 'jobs_killed': killed, 'jobs_cleaned': firstSize - len(runningWorkflows) }), mimetype='text/json')

@app.route('/api/killJob/<string:type>/<int:jobId>')
def killJob(type, jobId):
    key = "{}_{}".format(type, jobId)
    ret = {
        'id': jobId,
        'type': type,
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
    removeQuiet(getJobParamFilename(type, jobId))
    deleteJobFromDb(type, jobId)
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
    
    workflow = {
        'process': proc,
        'jobid': jobId,
        'jobtype': 'model',
        'jobname': 'model_publish',
        'machinename': HOSTNAME,
        'jobstatus': 0,
        'triggeredby': 'Anonymous',
        'taskstatus': 'Doing things...',
        'executorip': socket.gethostbyname(HOSTNAME)
    }
    runningWorkflows["{}_{}".format('model', jobId)] = workflow
    os.chdir(cwd)
    updateRemoteWorkflowInfo('model', jobId, workflow, True)
    return {
        'id': jobId,
        'type': 'model',
        'status': StatusCode.RUNNING,
        'links': {
            'get_status': '/api/jobStatus/model/{}'.format(jobId),
            'kill_job': '/api/killJob/model/{}'.format(jobId)
        }
    }