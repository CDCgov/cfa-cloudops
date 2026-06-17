dev_autoscale_formula = """
startingNumberOfVMs = 1;
// set the maximum number of nodes you would like to scale to
maxNumberofVMs = 10;
// Get pending tasks for the past 5 minutes.
$Samples = $PendingTasks.GetSamplePercent(TimeInterval_Minute * 5);
// If we have less than 70% data points, we use the last sample point, otherwise we use the maximum of
// last sample point and the history average.
$Tasks = $Samples < 70 ? max(0,$PendingTasks.GetSample(1)) : max( $PendingTasks.GetSample(1), avg($PendingTasks.GetSample(TimeInterval_Minute * 5)));
// If number of pending tasks is not 0, set targetVM to pending tasks, otherwise half of current dedicated.
$TargetVMs = $Tasks > 0? $Tasks:max(0, $TargetDedicated/2);
$TargetDedicated = 0.5*max(0,min($TargetVMs,maxNumberofVMs));
$TargetLowPriorityNodes = 0.5*max(0,min($TargetVMs,maxNumberofVMs));
// Set node deallocation mode - keep nodes active only until tasks finish
$NodeDeallocationOption = taskcompletion;
"""

# Production environment autoscale formula
prod_autoscale_formula = """
startingNumberOfVMs = 1;
// set the maximum number of nodes to scale to
maxNumberofVMs = 25;
// Get pending tasks for the past 5 minutes.
$Samples = $PendingTasks.GetSamplePercent(TimeInterval_Minute * 5);
// If we have less than 70% data points, we use the last sample point, otherwise we use the maximum of
// last sample point and the history average.
$Tasks = $Samples < 70 ? max(0,$PendingTasks.GetSample(1)) : max( $PendingTasks.GetSample(1), avg($PendingTasks.GetSample(TimeInterval_Minute * 5)));
// If number of pending tasks is not 0, set targetVM to pending tasks, otherwise half of current dedicated.
$TargetVMs = $Tasks > 0? $Tasks:max(0, $TargetDedicated/2);
$TargetDedicated = max(0,min($TargetVMs,maxNumberofVMs));
// Set node deallocation mode - keep nodes active only until tasks finish
$NodeDeallocationOption = taskcompletion;
"""
