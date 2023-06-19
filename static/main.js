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

const killJob = async (type, jobId) => {
    const response = await fetch(`/api/killJob/${type}/${jobId}`);
    const json = await response.json();
    await loadJobList();
}

const removeJob = async (type, jobId) => {
    const response = await fetch(`/api/removeJob/${type}/${jobId}`);
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

const menuHandler = (ev, jobtype, jobid, jobstatus) => {
    const menu = document.getElementById('actionMenu');
    const killLink =  document.getElementById('actionMenu_kill');
    const newKillLink = killLink.cloneNode(true);
    killLink.replaceWith(newKillLink);

    if (jobstatus != 0 && jobstatus !== null) {
        newKillLink.textContent ='Remove Job';
        newKillLink.addEventListener('click', () => {
            removeJob(jobtype, jobid);
        });
        
        const restartLink = document.getElementById('actionMenu_restart');
        const newRestartLink = restartLink.cloneNode(true);
        restartLink.replaceWith(newRestartLink);
        newRestartLink.style.display = 'block';
        newRestartLink.addEventListener('click', () => {
            restartJob(jobtype, jobid);
        });
        
        const logLink = document.getElementById('actionMenu_viewLogs')
        const newLogLink = logLink.cloneNode(true);
        logLink.replaceWith(newLogLink);
        newLogLink.style.display = 'block';
        newLogLink.addEventListener('click', () => {
            viewLogs(jobtype, jobid);
        });
    }
    else {
        newKillLink.addEventListener('click', () => {
            killJob(jobtype, jobid);
        });
        document.getElementById('actionMenu_kill').textContent ='Abort Job';
        document.getElementById('actionMenu_restart').style.display = 'none';
        document.getElementById('actionMenu_viewLogs').style.display = 'none';
    }
    const cells = document.querySelectorAll('.cell[data-id][data-type]');
    for (let cell of cells) {
        if (cell.dataset.type == jobtype && cell.dataset.id == jobid) {
            cell.classList.add('selected');
        } else {
            cell.classList.remove('selected');
        }
    }
    menu.style.left = `${ev.pageX}px`;
    menu.style.top = `${ev.pageY}px`;
    menu.style.display = 'grid';

    // console.log(jobname, jobid, jobstatus);
    ev.preventDefault();
    return false;
}
const clearContextMenu = () => {
    document.getElementById('actionMenu').style.display = 'none';
    const cells = document.querySelectorAll('.cell[data-id][data-type]');
    for (let cell of cells) {
        cell.classList.remove('selected');
    }
}
document.addEventListener('click', clearContextMenu);

const loadJobList = async () => {
    const response = await fetch('/api/getTrackedJobs');
    const json = await response.json();
    mainDiv.innerHTML = '';
    let output = `
    <div class="row">
        <div class="cell header">Status</div>
        <div class="cell header">Job ID</div>
        <div class="cell header">Start Time</div>
        <div class="cell header">Type</div>
        <div class="cell header">Username</div>
        <div class="cell header">Most Recent Step</div>
        <div class="cell header">Machine</div>
    `;
    for (let workflow of Object.values(json)) {
        output += `
            <div data-id="${workflow.jobid}" data-type="${workflow.jobtype}" class="cell bold ${workflow.jobstatus < 0 ? 'redbg white' : workflow.jobstatus > 0 ? 'greenbg white' : 'bluebg'}">${capitalize(Status[workflow.jobstatus])}</div>
            <div data-id="${workflow.jobid}" data-type="${workflow.jobtype}" class="cell">${workflow.jobid}</div>
            <div data-id="${workflow.jobid}" data-type="${workflow.jobtype}" class="cell timestamp">${workflow.starttime}</div>
            <div data-id="${workflow.jobid}" data-type="${workflow.jobtype}" class="cell">${capitalize(workflow.jobname)}</div>
            <div data-id="${workflow.jobid}" data-type="${workflow.jobtype}" class="cell">${workflow.triggeredby}</div>
            <div data-id="${workflow.jobid}" data-type="${workflow.jobtype}" class="cell">${workflow.taskstatus}</div>
            <div data-id="${workflow.jobid}" data-type="${workflow.jobtype}" class="cell">${workflow.machinename} (${workflow.executorip})</div>
        `;
    }
    mainDiv.innerHTML = output + '</div>';

    for (let workflow of Object.values(json)) {
        const rowCells = document.querySelectorAll(`.cell[data-type="${workflow.jobtype}"][data-id="${workflow.jobid}"]`)
        for (let cell of rowCells) {
            cell.addEventListener('contextmenu', ev => menuHandler(ev, workflow.jobtype, workflow.jobid, workflow.jobstatus));
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