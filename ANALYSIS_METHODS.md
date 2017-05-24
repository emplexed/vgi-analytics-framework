# VGI Analytics Framework - Analysis methods

## VgiAnalysisUserPerAction
Parameter: `mergeActionTypes`: `true` if overall number of actions should be counted; `false`

This analysis type lists the number of actions per user.

## VgiAnalysisActionPerFeatureType
Distribution of action types per time period and features type

## VgiAnalysisActionPerType
Counts actions per action type and time period

## VgiAnalysisUserPerOperation
Parameter: `mergeOperationTypes`: `true` if overall number of actions should be counted; `false` if number of action per action type should be counted

Counts operations per user, operation type and time period

## VgiAnalysisOperationPerType
Counts operations per operation type and time period

## Features per geometry type
Counts actions per geometry type and feature type

## VgiAnalysisTags
Parameters: `tagKey`: `''` if tag keys should be analyzed; `tag key` (e.g. building) if tag values should be analyzed

Counts added tags per user, tag key and geometry type (n, w, or r)

## VgiAnalysisBatchGeneral
The summery includes some basic statistics for multiple setting profiles. Each pro-file triggers a VGI pipeline. The summary analysis collects the results from the other analysis types before those are re-initialized. Basic statistics include 
* Number of users
* Numbers of actions
* Average number of actions per user
* User who contributes most actions
* Number of actions which have been contributed by top user

## VgiAnalysisBatchContributor
User count and action count per analysis entry time period

## VgiAnalysisBatchUserActionType
Statistics include number of actions per 
* feature type,
* time period and
* action type

## VgiAnalysisActionDetails
Parameter: `includeOperationDetails`: `true` if also operation details should be written; `false` if only action details should be written

Prints all attributes of all actions and operations