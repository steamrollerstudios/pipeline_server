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

const loadJobList = async () => {
    const response = await fetch('/api/getTrackedJobs');
    const json = await response.json();
    mainDiv.innerHTML = '';
    let output = `
    <div class="row">
        <div class="cell header">Abort</div>
        <div class="cell header">Job ID</div>
        <div class="cell header">Type</div>
        <div class="cell header">Status</div>
    `;
    for (let [key, workflow] of Object.entries(json)) {
        output += `
            <div class="cell">X</div>
            <div class="cell">${workflow.jobId}</div>
            <div class="cell">${workflow.type}</div>
            <div class="cell">${capitalize(Status[workflow.status])}</div>
        `;
    }
    mainDiv.innerHTML = output + '</div>';
}
loadJobList()