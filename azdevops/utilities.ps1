
function Wait-AzDevOpsPipelineTermination([int] $pipelineRunID) {
    $runInfo = $null
    do {
        start-sleep -seconds 30
        $runInfo = az pipelines runs show --id $pipelineRunID | ConvertFrom-Json
    } while ($runInfo.status -eq "inProgress")

    $status = $runInfo.status
    $result = $runInfo.result
    write-host "Pipeline $pipelineRunID $status with result $result"
}
