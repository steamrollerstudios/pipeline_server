const mainDiv = document.getElementById('main')

const Status = {
    2: 'ALREADY_COMPLETE',
    1: 'SUCCEEDED',
    0: 'RUNNING',
    null: 'RUNNING',
    '-1': 'UNTRACKED',
    '-2': 'FAILED'
}

const capitalize = str => {
    return str.split(/[ _]/g).map(word => word[0].toUpperCase() + word.slice(1).toLowerCase()).join(' ');
}

const killJob = async url => {
    const response = await fetch(url);
    const json = await response.json();
    await loadJobList();
}

const restartJob = async (type, jobId) => {
    const response = await fetch(`/api/restartJob/${type}/${jobId}`);
    const json = await response.json();
    console.log(json);
    await loadJobList();
}

const viewLogs = async (type, jobId) => {
    window.open(`/api/viewLogs/${type}/${jobId}`, '_blank');
}

const loadJobList = async () => {
    const response = await fetch('/api/getTrackedJobs');
    const json = await response.json();
    mainDiv.innerHTML = '';
    let output = `
    <div class="row">
        <div class="cell header">Actions</div>
        <div class="cell header">Job ID</div>
        <div class="cell header">Start Time</div>
        <div class="cell header">Type</div>
        <div class="cell header">Username</div>
        <div class="cell header">Status</div>
        <div class="cell header">Machine</div>
    `;
    for (let workflow of Object.values(json)) {
        output += `
            <div class="cell">
                <span class="link" id="killLink${workflow.jobid}">❌</span> 
                ${workflow.jobstatus != 0 && workflow.jobstatus !== null ? `<span class="link" id="restartLink${workflow.jobid}">🔃</span>` : ''}
                ${workflow.jobstatus != 0 && workflow.jobstatus !== null ? `<span class="link" id="logLink${workflow.jobid}">📄</span>` : ''}
            </div>
            <div class="cell">${workflow.jobid}</div>
            <div class="cell timestamp">${workflow.starttime}</div>
            <div class="cell">${capitalize(workflow.jobname)}</div>
            <div class="cell">${workflow.triggeredby}</div>
            <div class="cell">${capitalize(Status[workflow.jobstatus])}</div>
            <div class="cell">${workflow.machinename} (${workflow.executorip})</div>
        `;
    }
    mainDiv.innerHTML = output + '</div>';
    for (let workflow of Object.values(json)) {
        const killLink = document.getElementById(`killLink${workflow.jobid}`);
        killLink.title = (workflow.jobstatus != 0 && workflow.jobstatus !== null ? 'Remove' : 'Abort') + ' Job';
        killLink.addEventListener('click', () => {
            killJob(workflow.links.kill_job);
        });

        const restartLink = document.getElementById(`restartLink${workflow.jobid}`);
        if (restartLink) {
            restartLink.title = 'Restart Job'
            restartLink.addEventListener('click', () => {
                restartJob(workflow.jobtype, workflow.jobid);
            });
        }

        const logLink = document.getElementById(`logLink${workflow.jobid}`);
        if (logLink) {
            logLink.title = 'View Logs'
            logLink.addEventListener('click', () => {
                viewLogs(workflow.jobtype, workflow.jobid);
            });
        }
    }
}

loadJobList();
let reloadTimer = null;

loadJobList();
const autoReloadCheckbox = document.getElementById('autoReload');

const setAutoReload = async enabled => {
    autoReloadCheckbox.checked = enabled;
    if (enabled) {
        await loadJobList();
        reloadTimer = setInterval(loadJobList, 5000);
    } else {
        clearInterval(reloadTimer);
    }
    window.localStorage.setItem('autoReload', enabled ? '1' : '0');
}

autoReloadCheckbox.addEventListener('change', async () => {
    setAutoReload(autoReloadCheckbox.checked);
});

const reloadSetting = window.localStorage.getItem('autoReload');
if (reloadSetting === null) {
    setAutoReload(true);
} else {
    setAutoReload(reloadSetting === '1');
}