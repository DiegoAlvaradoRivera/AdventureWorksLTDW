
function Replace-Params($params, $jobTemplatePath){

    $jobDef = get-content $jobTemplatePath
    foreach ($elem in $params.GetEnumerator()) {
        $key = $elem.Name
        $value = $elem.Value
        $jobDef = $jobDef.replace("<$key>", $value)
    }
    $jobDef

}

function add-backslashes([string] $string){
    $string.replace('"', '\"')
}

function Convert-JsonObjectToString($jsonObj, [int] $depth = 10){
    if($jsonObj.length -eq 0) { "[]" }
    else {
        add-backslashes ($jsonObj | ConvertTo-Json -Depth $depth -Compress)
    }
}

function Assert-DatabricksConnection(){
    $output = databricks fs ls dbfs:/
    if(-not $?) { throw "Unable to connect to the Databricks workspace" }
}

function Get-DatabricksJobId([string] $name){
    <#
    Retrieve the id of a job given its name.
    Use the current workspace and token
    #>

    $result = (
        databricks jobs list --output table --version 2.1 | 
        foreach { 
            $record = $_ -split "\s+"
            @{id=$record[0]; name=$record[1]}
        } |
        where-object { $_.name -eq $name }
        ).id
    
    if($result -eq $null) { throw "Job '$name' does not exist" }
    $result
}

function Get-DatabricksInstancePoolId([string] $name){
    <#
    Retrieve the id of a job given its name.
    Use the current workspace and token
    #>

    $result = (
        databricks instance-pools list --output table | 
        select -skip 1 | 
        foreach { 
            $record = $_ -split "\s+"
            @{id=$record[0]; name=$record[1]}
        } |
        where-object { $_.name -eq $name }
        ).id
    
    if($result -eq $null) { throw "Instance pool '$name' does not exist" }
    $result
}

function Create-DatabricksJob([string] $name, [string] $definition) {
    <#
    
    #>
    $jobId = try { Get-DatabricksJobId $name } catch { $null }

    $result = if ($jobId -eq $null) {
        databricks jobs create --version=2.1 --json-file $definition
    } else {
        databricks jobs reset --version=2.1 --job-id $jobId --json-file $definition
    }

    $result
}

function Wait-DatabricksJobTermination($parentRunID){

    write-host "Waiting for job with parent run id $ParentRunId termination"

    $runMetadata = databricks runs get --run-id $parentRunId --version=2.1 | ConvertFrom-Json 
    $runLifeCycleState =  $runMetadata.state.life_cycle_state

    if( $runLifeCycleState -eq $null ) {
        throw "Could not retrieve the life_cycle_state of job run $parentRunID"
    }
    
    $finalStatuses = "TERMINATED","SKIPPED","INTERNAL_ERROR"
    while($runLifeCycleState -notin $finalStatuses){
        start-sleep -seconds 15
        $runMetadata = databricks runs get --run-id $parentRunId --version=2.1 | ConvertFrom-Json 
        $runLifeCycleState =  $runMetadata.state.life_cycle_state
        # write-host "job $parentRunId is $runLifeCycleState"
    }
    
    $runResultState = $runMetadata.state.result_state
    write-host "job $parentRunId $runLifeCycleState with result state $runResultState"

}
