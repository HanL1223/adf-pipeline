$opt = New-AdfPublishOption -FilterFilePath "./.azurepipelines/deploy-adf-filter.txt"
$opt.DeployGlobalParams = $true
$opt.DeleteNotInSource = $true
$opt.CreateNewInstance= $false


Publish-AdfV2FromJson -stage ci -rootfolder "./data-factory" -resourcegroupname cwr-ase-edp-ci-rg -datafactoryname cwr-ase-edp-ci-adf -location australiasoutheast -Option $opt -DryRun