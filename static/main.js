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
    return str.split(' ').map(word => word[0].toUpperCase() + word.slice(1).toLowerCase()).join(' ');
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

const loadJobList = async () => {
    const response = await fetch('/api/getTrackedJobs');
    const json = await response.json();
    mainDiv.innerHTML = '';
    let output = `
    <div class="row">
        <div class="cell header">Actions</div>
        <div class="cell header">Job ID</div>
        <div class="cell header">Type</div>
        <div class="cell header">Status</div>
        <div class="cell header">Machine</div>
    `;
    for (let workflow of Object.values(json)) {
        output += `
            <div class="cell"><span class="link" id="killLink${workflow.jobid}">X</span> <span class="link" id="restartLink${workflow.jobid}">🔃</span></div>
            <div class="cell">${workflow.jobid}</div>
            <div class="cell">${capitalize(workflow.jobtype)}</div>
            <div class="cell">${capitalize(Status[workflow.jobstatus])}</div>
            <div class="cell">${workflow.machinename} (${workflow.executorip})</div>
        `;
    }
    mainDiv.innerHTML = output + '</div>';
    for (let workflow of Object.values(json)) {
        document.getElementById(`killLink${workflow.jobid}`).addEventListener('click', () => {
            killJob(workflow.links.kill_job)
        });

        document.getElementById(`restartLink${workflow.jobid}`).addEventListener('click', () => {
           restartJob(workflow.type, workflow.jobId)
        });
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