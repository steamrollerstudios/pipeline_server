from flask import Flask, request, Response, render_template
from flask_cors import CORS
import sys
import json
import os
import random
import subprocess
from enum import Enum, IntEnum
import socket
from threading import Thread
import time

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
    existingJobsInDb = set()
    for workflow in results:
        key = "{}_{}".format(workflow['jobtype'], workflow['jobid'])
        existingJobsInDb.add(key)
        entry = runningWorkflows[key] if key in runningWorkflows else dict()
        runningWorkflows[key] = entry
        for column in workflow:
            entry[column] = workflow[column] if isinstance(workflow[column], str) else workflow[column]
    for (key, workflow) in runningWorkflows.copy().items():
        if key not in existingJobsInDb:
            killJob(workflow['jobtype'], workflow['jobid'])

    pipeline_db.commit()

def ensureCorrectColumns(data):
    keys = [k for k in data.keys()]
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
        'processstatus': None,
        'machinename': HOSTNAME,
        'triggeredby': 'Anonymous',
        'taskstatus': 'Running...',
        'executorip': '0.0.0.0',
        'logs': None
    }
    result.update(data)
    return result

def updateRemoteWorkflowInfo(type, jobId, data, includeDefaults = False, insertNew = False):
    if includeDefaults:
        result = getUpdatedDefaultWorkflow(type, jobId, data)
    else:
        result = dict()
        result.update(data)
    result = ensureCorrectColumns(result)
    columns = result.keys()
    values = tuple(result[column] for column in columns)
    if insertNew:
        statement = "INSERT INTO {} (%s) VALUES %s".format(ConstantPaths.PIPELINE_JOBS_TABLE)
        prepared_statement = dbcursor.mogrify(statement, (AsIs(', '.join(columns)), values))
    else:
        if len(values) > 1:
            statement = "UPDATE {} SET (%s) = %s WHERE jobid = {} AND jobtype='{}'".format(ConstantPaths.PIPELINE_JOBS_TABLE, str(jobId), type)
        else:
            statement = "UPDATE {} SET (%s) = ROW(%s) WHERE jobid = {} AND jobtype='{}'".format(ConstantPaths.PIPELINE_JOBS_TABLE, str(jobId), type)
        prepared_statement = dbcursor.mogrify(statement, (AsIs(', '.join(columns)), values))
    dbcursor.execute(prepared_statement)
    pipeline_db.commit()

def processStatusToJobStatus(status):
        if status is None:
            return StatusCode.RUNNING
        elif status != 0:
            return StatusCode.FAILED
        else:
            return StatusCode.SUCCEEDED

def getStatus(key):
    parts = key.split('_')
    type = parts[0]
    jobId = parts[1]
    if key not in runningWorkflows:
        return -1
    elif 'process' not in runningWorkflows[key]:
        return runningWorkflows[key]['jobstatus']
    else:
        status = runningWorkflows[key]['process'].poll()
        jobStatus = processStatusToJobStatus(status)
        runningWorkflows[key]['jobstatus'] = jobStatus
        updateRemoteWorkflowInfo(type, jobId, { 'processstatus': status, 'jobstatus': jobStatus })
        return jobStatus

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
        "username": "local.kevin.burns",
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
        jobStatus = getStatus(key)
        returnDict[key]['jobstatus'] = jobStatus
        returnDict[key]['links'] = {
            'get_status': '/api/jobStatus/{}/{}'.format(returnDict[key]['jobtype'], returnDict[key]['jobid']),
            'kill_job': '/api/killJob/{}/{}'.format(returnDict[key]['jobtype'], returnDict[key]['jobid'])
        }
        returnDict[key].pop('process', -1)
        returnDict[key].pop('logs')

    return Response(json.dumps(returnDict, default=str), mimetype='text/json')

@app.route('/api/restartJob/<string:type>/<int:jobId>')
def restartJob(type, jobId):
    jobFile = getJobParamFilename(type, jobId)
    if not os.path.exists(jobFile):
        return Response(json.dumps({'error': '-1'}), mimetype='text/json')
    deleteJobFromDb(type, jobId)
    with open(jobFile, 'r') as f:
        params = json.load(f)
    
    result = dict()
    if type == 'model':
        result = triggerModelPublish(jobId, params['repo'], params['username'])

    return Response(json.dumps(result), mimetype='text/json')

@app.route('/api/publishModel', methods=['POST'])
def runModelPublish():
    jobId = random.randint(1, 99999)
    jobFile = getJobParamFilename('model', jobId)
    while os.path.exists(jobFile):
        jobId = random.randint(1, 99999)
        jobFile = getJobParamFilename('model', jobId)
    with open(jobFile, 'w') as f:
        json.dump(request.form, f)

    ret = triggerModelPublish(jobId, request.form['repo'], request.form['username'])

    return Response(json.dumps(ret), mimetype='text/json')

@app.route('/api/viewLogs/<string:type>/<int:jobId>')
def viewLogs(type, jobId):
    statement = "SELECT logs FROM {} WHERE jobtype=%s AND jobid=%s".format(ConstantPaths.PIPELINE_JOBS_TABLE)

    dbcursor.execute(statement, (type, jobId))
    result = dbcursor.fetchone()
    if result:
        returnValue = result['logs']
    else:
        returnValue = 'No logs found.'
    pipeline_db.commit()
    return "<pre>{}</pre>".format(returnValue)

@app.route('/api/jobStatus/<string:type>/<int:jobId>')
def jobStatus(type, jobId):
    updateLocalWorkflowInfo()
    ret = {
        'jobid': jobId,
        'jobtype': type,
        'jobstatus': StatusCode.RUNNING,
        'links': {
            'get_status': '/api/jobStatus/{}/{}'.format(type, jobId),
            'kill_job': '/api/killJob/{}/{}'.format(type, jobId)
        }
    }
    key = "{}_{}".format(type, jobId)
    if key not in runningWorkflows:
        ret['jobstatus'] = StatusCode.UNTRACKED
    else:
        jobStatus = getStatus(key)
        ret['jobstatus'] = jobStatus
    return Response(json.dumps(ret), mimetype='text/json')

@app.route('/api/cleanJobs/<string:type>', defaults={'forcekill': 0})
@app.route('/api/cleanJobs/<string:type>/<int:forcekill>')
def cleanJobs(type, forcekill):
    updateLocalWorkflowInfo()
    firstSize = len(runningWorkflows)
    killed = 0
    for (key, workflow) in runningWorkflows.copy().items():
        if not key.startswith(type + '_') or not key in runningWorkflows:
            continue
        jobStatus = getStatus(key)
        if forcekill and jobStatus == StatusCode.RUNNING:
            killed += 1
            workflow['process'].terminate()
            removeQuiet(getJobParamFilename(type, workflow['jobid']))
            deleteJobFromDb(type, workflow['jobid'])
            runningWorkflows.pop(key, -1)
        elif jobStatus != StatusCode.RUNNING:
            removeQuiet(getJobParamFilename(type, workflow['jobid']))
            deleteJobFromDb(type, workflow['jobid'])
            runningWorkflows.pop(key, -1)
    return Response(json.dumps({ 'jobs_killed': killed, 'jobs_cleaned': firstSize - len(runningWorkflows) }), mimetype='text/json')

@app.route('/api/killJob/<string:type>/<int:jobId>')
def killJob(type, jobId):
    key = "{}_{}".format(type, jobId)
    ret = {
        'jobid': jobId,
        'jobtype': type,
        'jobstatus': StatusCode.RUNNING,
        'links': {
            'get_status': '/api/jobStatus/{}/{}'.format(type, jobId),
            'kill_job': '/api/killJob/{}/{}'.format(type, jobId)
        }
    }
    if key not in runningWorkflows:
        ret['jobstatus'] = StatusCode.UNTRACKED
    else:
        jobStatus = getStatus(key)
        if jobStatus == StatusCode.RUNNING:
            runningWorkflows[key]['process'].terminate()
            ret['jobstatus'] = StatusCode.SUCCEEDED
        elif jobStatus == StatusCode.SUCCEEDED:
            ret['jobstatus'] = StatusCode.ALREADY_COMPLETE
        else:
            ret['jobstatus'] = StatusCode.FAILED
    removeQuiet(getJobParamFilename(type, jobId))
    deleteJobFromDb(type, jobId)
    runningWorkflows.pop(key, -1)
    return Response(json.dumps(ret), mimetype='text/json')

def threaddedExecutor(callback, *subprocessArgs, **subprocessKwargs):
    proc = subprocess.Popen(*subprocessArgs, **subprocessKwargs)
    def pollAndWait():
        while True:
            time.sleep(5)
            status = proc.poll()
            if status is not None:
                fullLogs = ''
                with proc.stdout as output:
                    fullLogs += output.read().decode()
                errorLog = ''
                with proc.stderr as errors:
                    errorLog += errors.read().decode()
                if errorLog:
                    fullLogs += '=========================\n' + errorLog
                callback(status, fullLogs)
                break

    thread = Thread(target=pollAndWait)
    thread.start()
    return proc

def updateRemoteJobStatus(type, jobId, processStatus, fullLogs = None):
    status = processStatusToJobStatus(processStatus)
    updateRemoteWorkflowInfo(type, jobId, {'jobstatus': status, 'processstatus': processStatus, 'logs': fullLogs})

def triggerModelPublish(jobId, repo, username = 'Anonymous'):
    cwd = os.getcwd()
    os.chdir(repo)
    plastic = Workspace()
    plastic.update_workspace(undo_pending = True)
    os.chdir(ConstantPaths.LUIGI_TASK_PATH)
    # subprocess.run(['mayapy', os.path.abspath(os.path.join(ConstantPaths.LUIGI_TASK_PATH, 'runTasks.py')), str(jobId)], stderr=subprocess.STDOUT, check=True)

    proc = threaddedExecutor(lambda status, fullLogs: updateRemoteJobStatus('model', jobId, status, fullLogs), ['mayapy', os.path.abspath(os.path.join(ConstantPaths.LUIGI_TASK_PATH, 'runTasks.py')), str(jobId)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    workflow = {
        'process': proc,
        'jobid': jobId,
        'jobtype': 'model',
        'jobname': 'model_publish',
        'machinename': HOSTNAME,
        'jobstatus': 0,
        'processstatus': None,
        'triggeredby': username,
        'taskstatus': 'Doing things...',
        'executorip': socket.gethostbyname(HOSTNAME),
        'logs': None
    }
    runningWorkflows["{}_{}".format('model', jobId)] = workflow
    os.chdir(cwd)
    updateRemoteWorkflowInfo('model', jobId, workflow, True, True)
    return {
        'jobid': jobId,
        'jobtype': 'model',
        'jobstatus': StatusCode.RUNNING,
        'links': {
            'get_status': '/api/jobStatus/model/{}'.format(jobId),
            'kill_job': '/api/killJob/model/{}'.format(jobId)
        }
    }