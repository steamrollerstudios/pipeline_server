from flask import Flask, request, Response, render_template
from flask_cors import CORS
import sys
import json
import os
import subprocess
from enum import Enum, IntEnum
import socket
from threading import Thread
import requests
import tempfile

from steamroller.plasticAPI.workspace import Workspace

sys.path.insert(0, r'{}\Lib\site-packages'.format(os.environ['CONDA_PREFIX']))
import psycopg2
import psycopg2.extras
from psycopg2.extensions import AsIs

app = Flask(__name__)
CORS(app)

pipeline_db = psycopg2.connect(database="pipelinejobs",
                        host="10.100.0.48",
                        user="steamroller",
                        password="!uQ7Br&wratR",
                        port="5432")

runningWorkflows = dict()

class StatusCode(IntEnum):
    ALREADY_COMPLETE = 2
    SUCCEEDED = 1
    RUNNING = 0
    UNTRACKED = -1
    FAILED = -2

class ConstantPaths(str, Enum):
    LUIGI_TASK_PATH = r"D:\SteamrollerDev\developerpackages\steamroller\steamroller.tools.publisher\python\steamroller\tools\publisher",
    PIPELINE_JOBS_TABLE = 'public."jobs"'

HOSTNAME = socket.gethostname()

def getDbCursor():
    return pipeline_db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

with getDbCursor() as columnReader:
    columnReader.execute('SELECT * FROM {}'.format(ConstantPaths.PIPELINE_JOBS_TABLE))
    PIPELINE_COLUMNS = tuple(col.name for col in columnReader.description)
PROCESS_POLL_INTERVAL = 4

def deleteJobFromDb(type, jobId):
    with getDbCursor() as deleteCursor:
        statement = "DELETE FROM {} WHERE jobid = {} AND jobtype = '{}'".format(ConstantPaths.PIPELINE_JOBS_TABLE, jobId, type)
        deleteCursor.execute(statement)
        pipeline_db.commit()

def updateLocalWorkflowInfo():
    with getDbCursor() as updateCursor:
        updateCursor.execute('SELECT * FROM {}'.format(ConstantPaths.PIPELINE_JOBS_TABLE))
        if updateCursor.description is None:
            results = []
        else:
            try:
                results = updateCursor.fetchall()
            except:
                results = []
        pipeline_db.commit()

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
            removeJob(workflow['jobtype'], workflow['jobid'])

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
        'taskstatus': ['Job Starting...'],
        'luigistatus': None,
        'executorip': '0.0.0.0',
        'logs': None
    }
    result.update(data)
    return result

def getResponseLinks(type, jobId):
    return {
            'get_status': '/api/jobStatus/{}/{}'.format(type, jobId),
            'kill_job': '/api/killJob/{}/{}'.format(type, jobId),
            'remove_job': '/api/removeJob/{}/{}'.format(type, jobId),
            'restart_job': '/api/restartJob/{}/{}'.format(type, jobId)
    }

def updateRemoteWorkflowInfo(type, jobId, data, includeDefaults = False, insertNew = False):
    if includeDefaults:
        result = getUpdatedDefaultWorkflow(type, jobId, data)
    else:
        result = dict()
    result.update(data)
    result = ensureCorrectColumns(result)
    columns = result.keys()
    values = tuple(result[column] for column in columns)
    with getDbCursor() as addCursor:
        if insertNew:
            statement = "INSERT INTO {} (%s) VALUES %s".format(ConstantPaths.PIPELINE_JOBS_TABLE)
            prepared_statement = addCursor.mogrify(statement, (AsIs(', '.join(columns)), values))
        else:
            if len(values) > 1:
                statement = "UPDATE {} SET (%s) = %s WHERE jobid = {} AND jobtype='{}'".format(ConstantPaths.PIPELINE_JOBS_TABLE, str(jobId), type)
            else:
                statement = "UPDATE {} SET (%s) = ROW(%s) WHERE jobid = {} AND jobtype='{}'".format(ConstantPaths.PIPELINE_JOBS_TABLE, str(jobId), type)
            prepared_statement = addCursor.mogrify(statement, (AsIs(', '.join(columns)), values))
        try:
            addCursor.execute(prepared_statement)
            pipeline_db.commit()
            return True
        except:
            pipeline_db.rollback()
            return False

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
    elif 'process' not in runningWorkflows[key] or runningWorkflows[key]['process'] is None:
        return runningWorkflows[key]['jobstatus'] if runningWorkflows[key]['luigistatus'] == 1 else -2
    else:
        status = runningWorkflows[key]['process'].poll()
        jobStatus = processStatusToJobStatus(status)
        runningWorkflows[key]['jobstatus'] = jobStatus
        updateRemoteWorkflowInfo(type, jobId, { 'processstatus': status, 'jobstatus': jobStatus })
        return jobStatus if status != 0 else 1 if runningWorkflows[key]['luigistatus'] == 1 else -2

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
        "taskName": "serrano ship",
        "username": "local.kevin.burns",
        "checkInComment": "Testing full publish pipeline from JSON data"
    }
    output = '<form id="publishForm">'
    
    for key, val in data.items():
        if key == 'publishNotes':
            output += '<textarea name="{}">{}</textarea>'.format(key, val)
        else:
            output += '<input type="{}" name="{}" value="{}">'.format('text' if isinstance(val, str) else 'number', key, val)
    output += '<button>Submit</button></form>'
    output += '''
    <script type="text/javascript">
        const theForm = document.getElementById("publishForm");
        theForm.addEventListener("submit", e => {
            const current = theForm.innerHTML;
            e.preventDefault();
            const data = new FormData(theForm);
            const json = Object.fromEntries(data);
            theForm.innerHTML = 'Submitting job...';
            fetch('/api/publishModel', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(json)
            })
            .then(res => res.json())
            .then(jsonResponse => theForm.innerHTML = '<pre>' + JSON.stringify(jsonResponse) + '</pre>')
            .catch(e => theForm.innerHTML = current);
            return false;
        });
    </script>
    '''
    return output

@app.route('/api/getTrackedJobs')
def getTrackedJobs():
    updateLocalWorkflowInfo()
    returnDict = dict()
    for (key, workflow) in runningWorkflows.items():
        returnDict[key] = workflow.copy()
        jobStatus = getStatus(key)
        returnDict[key]['jobstatus'] = jobStatus
        returnDict[key]['links'] = getResponseLinks(returnDict[key]['jobtype'], returnDict[key]['jobid'])
        returnDict[key].pop('process', -1)
        returnDict[key].pop('logs')

    return Response(json.dumps(returnDict, default=str), mimetype='text/json')

@app.route('/api/restartJob/<string:type>/<int:jobId>')
def restartJob(type, jobId):
    with getDbCursor() as fetchCursor:
        statement = "SELECT machinename, executorip FROM {} WHERE jobtype=%s AND jobid=%s".format(ConstantPaths.PIPELINE_JOBS_TABLE)
        fetchCursor.execute(statement, (type, jobId))
        result = fetchCursor.fetchone()
        pipeline_db.commit()

    if not result:
        return Response(json.dumps({'error': -1}), mimetype='text/json')
    
    if result['machinename'] == HOSTNAME and result['executorip'] == socket.gethostbyname(HOSTNAME):
        jobFile = getJobParamFilename(type, jobId)
        if not os.path.exists(jobFile):
            return Response(json.dumps({'error': '-1'}), mimetype='text/json')
        deleteJobFromDb(type, jobId)
        runningWorkflows.pop("{}_{}".format(type, jobId))
        with open(jobFile, 'r') as f:
            params = json.load(f)
        
        result = dict()
        if type == 'model':
            result = triggerModelPublish(jobId, params['repo'], params['username'])

        return Response(json.dumps(result), mimetype='text/json')
    else:
        return requests.get("http://{}/api/restartJob/{}/{}".format(result['executorip'], type, jobId)).content

@app.route('/api/appendTaskStatus/<string:type>/<int:jobId>', methods=['POST'])
def appendTaskStatus(type, jobId):
    success = False
    with getDbCursor() as updateCursor:
        statement = "UPDATE {} SET taskstatus = array_append(taskstatus, %s) WHERE jobid=%s AND jobtype=%s".format(ConstantPaths.PIPELINE_JOBS_TABLE)
        try:
            updateCursor.execute(statement, (request.json['taskstatus'], jobId, type))
            pipeline_db.commit()
            success = True
        except Exception as e:
            pipeline_db.rollback()
            success = False

    return Response(json.dumps({'success': success }), mimetype='text/json')

@app.route('/api/updateLuigiStatus/<string:type>/<int:jobId>', methods=['POST'])
def updateLuigiStatus(type, jobId):
    success = updateRemoteWorkflowInfo(type, jobId, {
        'luigistatus': request.json['luigistatus']
    })
    return Response(json.dumps({'success': success }), mimetype='text/json')

def getNextJobId(type):
    with getDbCursor() as fetchCursor:
        fetchCursor.execute('SELECT jobid FROM {} WHERE jobtype=%s'.format(ConstantPaths.PIPELINE_JOBS_TABLE), (type,))
        results = fetchCursor.fetchall()
        ids = [result['jobid'] for result in results]
        jobId = 1
        while jobId in ids:
            jobId += 1
        pipeline_db.commit()
    return jobId

@app.route('/api/publishModel', methods=['POST'])
def runModelPublish():
    jobId = getNextJobId('model')
    jobFile = getJobParamFilename('model', jobId)
    while os.path.exists(jobFile):
        jobId += 1
        jobFile = getJobParamFilename('model', jobId)
    with open(jobFile, 'w') as f:
        json.dump(request.json, f)

    ret = triggerModelPublish(jobId, request.json['repo'], request.json['username'], request.json['taskName'])

    return Response(json.dumps(ret), mimetype='text/json')

@app.route('/api/viewLogs/<string:type>/<int:jobId>')
def viewLogs(type, jobId):
    statement = "SELECT logs FROM {} WHERE jobtype=%s AND jobid=%s".format(ConstantPaths.PIPELINE_JOBS_TABLE)
    with getDbCursor() as logCursor:
        logCursor.execute(statement, (type, jobId))
        result = logCursor.fetchone()
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
        'links': getResponseLinks(type, jobId)
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
        if forcekill or jobStatus != StatusCode.RUNNING:
            if jobStatus == StatusCode.RUNNING:
                killed += 1
            removeJob(type, workflow['jobid'])
            runningWorkflows.pop(key, -1)

    return Response(json.dumps({ 'jobs_killed': killed, 'jobs_cleaned': firstSize - len(runningWorkflows) }), mimetype='text/json')

@app.route('/api/removeJob/<string:type>/<int:jobId>')
def removeJob(type, jobId):
    with getDbCursor() as fetchCursor:
        statement = "SELECT machinename, executorip FROM {} WHERE jobtype=%s AND jobid=%s".format(ConstantPaths.PIPELINE_JOBS_TABLE)
        fetchCursor.execute(statement, (type, jobId))
        try:
            result = fetchCursor.fetchone()
            pipeline_db.commit()
        except:
            result = None
            pipeline_db.rollback()

    if not result:
        return Response(json.dumps({'error': -1}), mimetype='text/json')
    
    if result['machinename'] == HOSTNAME and result['executorip'] == socket.gethostbyname(HOSTNAME):
        res = killJob(type, jobId)
        removeQuiet(getJobParamFilename(type, jobId))
        deleteJobFromDb(type, jobId)
        return res
    else:
        return requests.get("http://{}/api/removeJob/{}/{}".format(result['executorip'], type, jobId)).content

@app.route('/api/killJob/<string:type>/<int:jobId>')
def killJob(type, jobId):
    key = "{}_{}".format(type, jobId)
    with getDbCursor() as fetchCursor:
        statement = "SELECT machinename, executorip FROM {} WHERE jobtype=%s AND jobid=%s".format(ConstantPaths.PIPELINE_JOBS_TABLE)
        fetchCursor.execute(statement, (type, jobId))
        try:
            result = fetchCursor.fetchone()
            pipeline_db.commit()
        except:
            result = None
            pipeline_db.rollback()

    if not result:
        return Response(json.dumps({'error': -1}), mimetype='text/json')
    
    if result['machinename'] == HOSTNAME and result['executorip'] == socket.gethostbyname(HOSTNAME):
        ret = {
            'jobid': jobId,
            'jobtype': type,
            'jobstatus': StatusCode.RUNNING,
            'links': getResponseLinks(type, jobId)
        }
        if key not in runningWorkflows:
            ret['jobstatus'] = StatusCode.UNTRACKED
        else:
            jobStatus = getStatus(key)
            if jobStatus == StatusCode.RUNNING:
                runningWorkflows[key]['process'].kill()
                ret['jobstatus'] = StatusCode.SUCCEEDED
            elif jobStatus == StatusCode.SUCCEEDED:
                ret['jobstatus'] = StatusCode.ALREADY_COMPLETE
            else:
                ret['jobstatus'] = StatusCode.FAILED
        runningWorkflows.pop(key, -1)
        return Response(json.dumps(ret), mimetype='text/json')
    else:
        return requests.get("http://{}/api/killJob/{}/{}".format(result['executorip'], type, jobId)).content

def threaddedExecutor(callback, *subprocessArgs, **subprocessKwargs):
    logFile = tempfile.TemporaryFile(mode='w+', encoding='latin1')
    errLogFile = tempfile.TemporaryFile(mode='w+', encoding='latin1')
    proc = subprocess.Popen(
        *subprocessArgs,
        **subprocessKwargs,
        stdout=logFile,
        stderr=errLogFile,
        encoding='latin1',
        close_fds=False
    )
    def pollAndWait():
        fullLogs = ''
        errorLog = ''
        status = None
        while True:
            status = proc.poll()
            if status is not None:
                break
        
        logFile.seek(0)
        errLogFile.seek(0)
        fullLogs = logFile.read()
        errorLog = errLogFile.read()
        if errorLog:
            fullLogs += '=========================\n' + errorLog
        logFile.close()
        errLogFile.close()
        callback(status, fullLogs)

    thread = Thread(target=pollAndWait)
    thread.start()
    return proc

def updateRemoteJobStatus(type, jobId, processStatus, fullLogs = None):
    updateLocalWorkflowInfo()
    if processStatus != 0:
        status = processStatusToJobStatus(processStatus)
    else:
        status = 0 if runningWorkflows["{}_{}".format('model', jobId)]['luigistatus'] == 1 else -2
    updateRemoteWorkflowInfo(type, jobId, {'jobstatus': status, 'processstatus': processStatus, 'logs': fullLogs})

def triggerModelPublish(jobId, repo, username = 'Anonymous', taskname = 'Unknown Task'):
    cwd = os.getcwd()
    os.chdir(repo)
    plastic = Workspace()
    plastic.update_workspace(undo_pending = True)
    os.chdir(ConstantPaths.LUIGI_TASK_PATH)
    # subprocess.run(['mayapy', os.path.abspath(os.path.join(ConstantPaths.LUIGI_TASK_PATH, 'runTasks.py')), str(jobId)], stderr=subprocess.STDOUT, check=True)
    workflow = {
        'process': None,
        'jobid': jobId,
        'jobtype': 'model',
        'jobname': taskname + ' - Publish',
        'machinename': HOSTNAME,
        'jobstatus': 0,
        'processstatus': None,
        'triggeredby': username,
        'taskstatus': ['Job Starting...'],
        'luigistatus': None,
        'executorip': socket.gethostbyname(HOSTNAME),
        'logs': None
    }
    updateRemoteWorkflowInfo('model', jobId, workflow, True, True)
    runningWorkflows["{}_{}".format('model', jobId)] = workflow

    os.environ["PYTHONUNBUFFERED"] = "1"
    proc = threaddedExecutor(
        lambda status, fullLogs: updateRemoteJobStatus('model', jobId, status, fullLogs),
        ['mayapy', os.path.abspath(os.path.join(ConstantPaths.LUIGI_TASK_PATH, 'runTasks.py')), str(jobId)]
    )
    workflow['process'] = proc

    os.chdir(cwd)
    return {
        'jobid': jobId,
        'jobtype': 'model',
        'jobstatus': StatusCode.RUNNING,
        'links': getResponseLinks('model', jobId)
    }

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)