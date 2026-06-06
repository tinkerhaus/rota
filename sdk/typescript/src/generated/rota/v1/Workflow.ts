// Original file: proto/rota/v1/rota.proto

import type * as grpc from '@grpc/grpc-js'
import type { MethodDefinition } from '@grpc/proto-loader'
import type { CancelWorkflowRequest as _rota_v1_CancelWorkflowRequest, CancelWorkflowRequest__Output as _rota_v1_CancelWorkflowRequest__Output } from '../../rota/v1/CancelWorkflowRequest';
import type { CancelWorkflowResponse as _rota_v1_CancelWorkflowResponse, CancelWorkflowResponse__Output as _rota_v1_CancelWorkflowResponse__Output } from '../../rota/v1/CancelWorkflowResponse';
import type { GetWorkflowHistoryResponse as _rota_v1_GetWorkflowHistoryResponse, GetWorkflowHistoryResponse__Output as _rota_v1_GetWorkflowHistoryResponse__Output } from '../../rota/v1/GetWorkflowHistoryResponse';
import type { ListWorkflowRunsRequest as _rota_v1_ListWorkflowRunsRequest, ListWorkflowRunsRequest__Output as _rota_v1_ListWorkflowRunsRequest__Output } from '../../rota/v1/ListWorkflowRunsRequest';
import type { ListWorkflowRunsResponse as _rota_v1_ListWorkflowRunsResponse, ListWorkflowRunsResponse__Output as _rota_v1_ListWorkflowRunsResponse__Output } from '../../rota/v1/ListWorkflowRunsResponse';
import type { PollTaskRequest as _rota_v1_PollTaskRequest, PollTaskRequest__Output as _rota_v1_PollTaskRequest__Output } from '../../rota/v1/PollTaskRequest';
import type { PolledActivityTask as _rota_v1_PolledActivityTask, PolledActivityTask__Output as _rota_v1_PolledActivityTask__Output } from '../../rota/v1/PolledActivityTask';
import type { PolledWorkflowTask as _rota_v1_PolledWorkflowTask, PolledWorkflowTask__Output as _rota_v1_PolledWorkflowTask__Output } from '../../rota/v1/PolledWorkflowTask';
import type { RespondActivityTaskRequest as _rota_v1_RespondActivityTaskRequest, RespondActivityTaskRequest__Output as _rota_v1_RespondActivityTaskRequest__Output } from '../../rota/v1/RespondActivityTaskRequest';
import type { RespondActivityTaskResponse as _rota_v1_RespondActivityTaskResponse, RespondActivityTaskResponse__Output as _rota_v1_RespondActivityTaskResponse__Output } from '../../rota/v1/RespondActivityTaskResponse';
import type { RespondWorkflowTaskRequest as _rota_v1_RespondWorkflowTaskRequest, RespondWorkflowTaskRequest__Output as _rota_v1_RespondWorkflowTaskRequest__Output } from '../../rota/v1/RespondWorkflowTaskRequest';
import type { RespondWorkflowTaskResponse as _rota_v1_RespondWorkflowTaskResponse, RespondWorkflowTaskResponse__Output as _rota_v1_RespondWorkflowTaskResponse__Output } from '../../rota/v1/RespondWorkflowTaskResponse';
import type { SignalWorkflowRequest as _rota_v1_SignalWorkflowRequest, SignalWorkflowRequest__Output as _rota_v1_SignalWorkflowRequest__Output } from '../../rota/v1/SignalWorkflowRequest';
import type { SignalWorkflowResponse as _rota_v1_SignalWorkflowResponse, SignalWorkflowResponse__Output as _rota_v1_SignalWorkflowResponse__Output } from '../../rota/v1/SignalWorkflowResponse';
import type { StartWorkflowRequest as _rota_v1_StartWorkflowRequest, StartWorkflowRequest__Output as _rota_v1_StartWorkflowRequest__Output } from '../../rota/v1/StartWorkflowRequest';
import type { StartWorkflowResponse as _rota_v1_StartWorkflowResponse, StartWorkflowResponse__Output as _rota_v1_StartWorkflowResponse__Output } from '../../rota/v1/StartWorkflowResponse';
import type { WorkflowRun as _rota_v1_WorkflowRun, WorkflowRun__Output as _rota_v1_WorkflowRun__Output } from '../../rota/v1/WorkflowRun';
import type { WorkflowRunRef as _rota_v1_WorkflowRunRef, WorkflowRunRef__Output as _rota_v1_WorkflowRunRef__Output } from '../../rota/v1/WorkflowRunRef';

export interface WorkflowClient extends grpc.Client {
  CancelWorkflow(argument: _rota_v1_CancelWorkflowRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_CancelWorkflowResponse__Output>): grpc.ClientUnaryCall;
  CancelWorkflow(argument: _rota_v1_CancelWorkflowRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_CancelWorkflowResponse__Output>): grpc.ClientUnaryCall;
  CancelWorkflow(argument: _rota_v1_CancelWorkflowRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_CancelWorkflowResponse__Output>): grpc.ClientUnaryCall;
  CancelWorkflow(argument: _rota_v1_CancelWorkflowRequest, callback: grpc.requestCallback<_rota_v1_CancelWorkflowResponse__Output>): grpc.ClientUnaryCall;
  cancelWorkflow(argument: _rota_v1_CancelWorkflowRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_CancelWorkflowResponse__Output>): grpc.ClientUnaryCall;
  cancelWorkflow(argument: _rota_v1_CancelWorkflowRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_CancelWorkflowResponse__Output>): grpc.ClientUnaryCall;
  cancelWorkflow(argument: _rota_v1_CancelWorkflowRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_CancelWorkflowResponse__Output>): grpc.ClientUnaryCall;
  cancelWorkflow(argument: _rota_v1_CancelWorkflowRequest, callback: grpc.requestCallback<_rota_v1_CancelWorkflowResponse__Output>): grpc.ClientUnaryCall;
  
  GetWorkflowHistory(argument: _rota_v1_WorkflowRunRef, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_GetWorkflowHistoryResponse__Output>): grpc.ClientUnaryCall;
  GetWorkflowHistory(argument: _rota_v1_WorkflowRunRef, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_GetWorkflowHistoryResponse__Output>): grpc.ClientUnaryCall;
  GetWorkflowHistory(argument: _rota_v1_WorkflowRunRef, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_GetWorkflowHistoryResponse__Output>): grpc.ClientUnaryCall;
  GetWorkflowHistory(argument: _rota_v1_WorkflowRunRef, callback: grpc.requestCallback<_rota_v1_GetWorkflowHistoryResponse__Output>): grpc.ClientUnaryCall;
  getWorkflowHistory(argument: _rota_v1_WorkflowRunRef, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_GetWorkflowHistoryResponse__Output>): grpc.ClientUnaryCall;
  getWorkflowHistory(argument: _rota_v1_WorkflowRunRef, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_GetWorkflowHistoryResponse__Output>): grpc.ClientUnaryCall;
  getWorkflowHistory(argument: _rota_v1_WorkflowRunRef, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_GetWorkflowHistoryResponse__Output>): grpc.ClientUnaryCall;
  getWorkflowHistory(argument: _rota_v1_WorkflowRunRef, callback: grpc.requestCallback<_rota_v1_GetWorkflowHistoryResponse__Output>): grpc.ClientUnaryCall;
  
  GetWorkflowRun(argument: _rota_v1_WorkflowRunRef, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_WorkflowRun__Output>): grpc.ClientUnaryCall;
  GetWorkflowRun(argument: _rota_v1_WorkflowRunRef, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_WorkflowRun__Output>): grpc.ClientUnaryCall;
  GetWorkflowRun(argument: _rota_v1_WorkflowRunRef, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_WorkflowRun__Output>): grpc.ClientUnaryCall;
  GetWorkflowRun(argument: _rota_v1_WorkflowRunRef, callback: grpc.requestCallback<_rota_v1_WorkflowRun__Output>): grpc.ClientUnaryCall;
  getWorkflowRun(argument: _rota_v1_WorkflowRunRef, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_WorkflowRun__Output>): grpc.ClientUnaryCall;
  getWorkflowRun(argument: _rota_v1_WorkflowRunRef, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_WorkflowRun__Output>): grpc.ClientUnaryCall;
  getWorkflowRun(argument: _rota_v1_WorkflowRunRef, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_WorkflowRun__Output>): grpc.ClientUnaryCall;
  getWorkflowRun(argument: _rota_v1_WorkflowRunRef, callback: grpc.requestCallback<_rota_v1_WorkflowRun__Output>): grpc.ClientUnaryCall;
  
  ListWorkflowRuns(argument: _rota_v1_ListWorkflowRunsRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_ListWorkflowRunsResponse__Output>): grpc.ClientUnaryCall;
  ListWorkflowRuns(argument: _rota_v1_ListWorkflowRunsRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_ListWorkflowRunsResponse__Output>): grpc.ClientUnaryCall;
  ListWorkflowRuns(argument: _rota_v1_ListWorkflowRunsRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_ListWorkflowRunsResponse__Output>): grpc.ClientUnaryCall;
  ListWorkflowRuns(argument: _rota_v1_ListWorkflowRunsRequest, callback: grpc.requestCallback<_rota_v1_ListWorkflowRunsResponse__Output>): grpc.ClientUnaryCall;
  listWorkflowRuns(argument: _rota_v1_ListWorkflowRunsRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_ListWorkflowRunsResponse__Output>): grpc.ClientUnaryCall;
  listWorkflowRuns(argument: _rota_v1_ListWorkflowRunsRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_ListWorkflowRunsResponse__Output>): grpc.ClientUnaryCall;
  listWorkflowRuns(argument: _rota_v1_ListWorkflowRunsRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_ListWorkflowRunsResponse__Output>): grpc.ClientUnaryCall;
  listWorkflowRuns(argument: _rota_v1_ListWorkflowRunsRequest, callback: grpc.requestCallback<_rota_v1_ListWorkflowRunsResponse__Output>): grpc.ClientUnaryCall;
  
  PollActivityTask(argument: _rota_v1_PollTaskRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_PolledActivityTask__Output>): grpc.ClientUnaryCall;
  PollActivityTask(argument: _rota_v1_PollTaskRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_PolledActivityTask__Output>): grpc.ClientUnaryCall;
  PollActivityTask(argument: _rota_v1_PollTaskRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_PolledActivityTask__Output>): grpc.ClientUnaryCall;
  PollActivityTask(argument: _rota_v1_PollTaskRequest, callback: grpc.requestCallback<_rota_v1_PolledActivityTask__Output>): grpc.ClientUnaryCall;
  pollActivityTask(argument: _rota_v1_PollTaskRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_PolledActivityTask__Output>): grpc.ClientUnaryCall;
  pollActivityTask(argument: _rota_v1_PollTaskRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_PolledActivityTask__Output>): grpc.ClientUnaryCall;
  pollActivityTask(argument: _rota_v1_PollTaskRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_PolledActivityTask__Output>): grpc.ClientUnaryCall;
  pollActivityTask(argument: _rota_v1_PollTaskRequest, callback: grpc.requestCallback<_rota_v1_PolledActivityTask__Output>): grpc.ClientUnaryCall;
  
  PollWorkflowTask(argument: _rota_v1_PollTaskRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_PolledWorkflowTask__Output>): grpc.ClientUnaryCall;
  PollWorkflowTask(argument: _rota_v1_PollTaskRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_PolledWorkflowTask__Output>): grpc.ClientUnaryCall;
  PollWorkflowTask(argument: _rota_v1_PollTaskRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_PolledWorkflowTask__Output>): grpc.ClientUnaryCall;
  PollWorkflowTask(argument: _rota_v1_PollTaskRequest, callback: grpc.requestCallback<_rota_v1_PolledWorkflowTask__Output>): grpc.ClientUnaryCall;
  pollWorkflowTask(argument: _rota_v1_PollTaskRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_PolledWorkflowTask__Output>): grpc.ClientUnaryCall;
  pollWorkflowTask(argument: _rota_v1_PollTaskRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_PolledWorkflowTask__Output>): grpc.ClientUnaryCall;
  pollWorkflowTask(argument: _rota_v1_PollTaskRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_PolledWorkflowTask__Output>): grpc.ClientUnaryCall;
  pollWorkflowTask(argument: _rota_v1_PollTaskRequest, callback: grpc.requestCallback<_rota_v1_PolledWorkflowTask__Output>): grpc.ClientUnaryCall;
  
  RespondActivityTask(argument: _rota_v1_RespondActivityTaskRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_RespondActivityTaskResponse__Output>): grpc.ClientUnaryCall;
  RespondActivityTask(argument: _rota_v1_RespondActivityTaskRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_RespondActivityTaskResponse__Output>): grpc.ClientUnaryCall;
  RespondActivityTask(argument: _rota_v1_RespondActivityTaskRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_RespondActivityTaskResponse__Output>): grpc.ClientUnaryCall;
  RespondActivityTask(argument: _rota_v1_RespondActivityTaskRequest, callback: grpc.requestCallback<_rota_v1_RespondActivityTaskResponse__Output>): grpc.ClientUnaryCall;
  respondActivityTask(argument: _rota_v1_RespondActivityTaskRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_RespondActivityTaskResponse__Output>): grpc.ClientUnaryCall;
  respondActivityTask(argument: _rota_v1_RespondActivityTaskRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_RespondActivityTaskResponse__Output>): grpc.ClientUnaryCall;
  respondActivityTask(argument: _rota_v1_RespondActivityTaskRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_RespondActivityTaskResponse__Output>): grpc.ClientUnaryCall;
  respondActivityTask(argument: _rota_v1_RespondActivityTaskRequest, callback: grpc.requestCallback<_rota_v1_RespondActivityTaskResponse__Output>): grpc.ClientUnaryCall;
  
  RespondWorkflowTask(argument: _rota_v1_RespondWorkflowTaskRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_RespondWorkflowTaskResponse__Output>): grpc.ClientUnaryCall;
  RespondWorkflowTask(argument: _rota_v1_RespondWorkflowTaskRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_RespondWorkflowTaskResponse__Output>): grpc.ClientUnaryCall;
  RespondWorkflowTask(argument: _rota_v1_RespondWorkflowTaskRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_RespondWorkflowTaskResponse__Output>): grpc.ClientUnaryCall;
  RespondWorkflowTask(argument: _rota_v1_RespondWorkflowTaskRequest, callback: grpc.requestCallback<_rota_v1_RespondWorkflowTaskResponse__Output>): grpc.ClientUnaryCall;
  respondWorkflowTask(argument: _rota_v1_RespondWorkflowTaskRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_RespondWorkflowTaskResponse__Output>): grpc.ClientUnaryCall;
  respondWorkflowTask(argument: _rota_v1_RespondWorkflowTaskRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_RespondWorkflowTaskResponse__Output>): grpc.ClientUnaryCall;
  respondWorkflowTask(argument: _rota_v1_RespondWorkflowTaskRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_RespondWorkflowTaskResponse__Output>): grpc.ClientUnaryCall;
  respondWorkflowTask(argument: _rota_v1_RespondWorkflowTaskRequest, callback: grpc.requestCallback<_rota_v1_RespondWorkflowTaskResponse__Output>): grpc.ClientUnaryCall;
  
  SignalWorkflow(argument: _rota_v1_SignalWorkflowRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_SignalWorkflowResponse__Output>): grpc.ClientUnaryCall;
  SignalWorkflow(argument: _rota_v1_SignalWorkflowRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_SignalWorkflowResponse__Output>): grpc.ClientUnaryCall;
  SignalWorkflow(argument: _rota_v1_SignalWorkflowRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_SignalWorkflowResponse__Output>): grpc.ClientUnaryCall;
  SignalWorkflow(argument: _rota_v1_SignalWorkflowRequest, callback: grpc.requestCallback<_rota_v1_SignalWorkflowResponse__Output>): grpc.ClientUnaryCall;
  signalWorkflow(argument: _rota_v1_SignalWorkflowRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_SignalWorkflowResponse__Output>): grpc.ClientUnaryCall;
  signalWorkflow(argument: _rota_v1_SignalWorkflowRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_SignalWorkflowResponse__Output>): grpc.ClientUnaryCall;
  signalWorkflow(argument: _rota_v1_SignalWorkflowRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_SignalWorkflowResponse__Output>): grpc.ClientUnaryCall;
  signalWorkflow(argument: _rota_v1_SignalWorkflowRequest, callback: grpc.requestCallback<_rota_v1_SignalWorkflowResponse__Output>): grpc.ClientUnaryCall;
  
  StartWorkflow(argument: _rota_v1_StartWorkflowRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_StartWorkflowResponse__Output>): grpc.ClientUnaryCall;
  StartWorkflow(argument: _rota_v1_StartWorkflowRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_StartWorkflowResponse__Output>): grpc.ClientUnaryCall;
  StartWorkflow(argument: _rota_v1_StartWorkflowRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_StartWorkflowResponse__Output>): grpc.ClientUnaryCall;
  StartWorkflow(argument: _rota_v1_StartWorkflowRequest, callback: grpc.requestCallback<_rota_v1_StartWorkflowResponse__Output>): grpc.ClientUnaryCall;
  startWorkflow(argument: _rota_v1_StartWorkflowRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_StartWorkflowResponse__Output>): grpc.ClientUnaryCall;
  startWorkflow(argument: _rota_v1_StartWorkflowRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_StartWorkflowResponse__Output>): grpc.ClientUnaryCall;
  startWorkflow(argument: _rota_v1_StartWorkflowRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_StartWorkflowResponse__Output>): grpc.ClientUnaryCall;
  startWorkflow(argument: _rota_v1_StartWorkflowRequest, callback: grpc.requestCallback<_rota_v1_StartWorkflowResponse__Output>): grpc.ClientUnaryCall;
  
}

export interface WorkflowHandlers extends grpc.UntypedServiceImplementation {
  CancelWorkflow: grpc.handleUnaryCall<_rota_v1_CancelWorkflowRequest__Output, _rota_v1_CancelWorkflowResponse>;
  
  GetWorkflowHistory: grpc.handleUnaryCall<_rota_v1_WorkflowRunRef__Output, _rota_v1_GetWorkflowHistoryResponse>;
  
  GetWorkflowRun: grpc.handleUnaryCall<_rota_v1_WorkflowRunRef__Output, _rota_v1_WorkflowRun>;
  
  ListWorkflowRuns: grpc.handleUnaryCall<_rota_v1_ListWorkflowRunsRequest__Output, _rota_v1_ListWorkflowRunsResponse>;
  
  PollActivityTask: grpc.handleUnaryCall<_rota_v1_PollTaskRequest__Output, _rota_v1_PolledActivityTask>;
  
  PollWorkflowTask: grpc.handleUnaryCall<_rota_v1_PollTaskRequest__Output, _rota_v1_PolledWorkflowTask>;
  
  RespondActivityTask: grpc.handleUnaryCall<_rota_v1_RespondActivityTaskRequest__Output, _rota_v1_RespondActivityTaskResponse>;
  
  RespondWorkflowTask: grpc.handleUnaryCall<_rota_v1_RespondWorkflowTaskRequest__Output, _rota_v1_RespondWorkflowTaskResponse>;
  
  SignalWorkflow: grpc.handleUnaryCall<_rota_v1_SignalWorkflowRequest__Output, _rota_v1_SignalWorkflowResponse>;
  
  StartWorkflow: grpc.handleUnaryCall<_rota_v1_StartWorkflowRequest__Output, _rota_v1_StartWorkflowResponse>;
  
}

export interface WorkflowDefinition extends grpc.ServiceDefinition {
  CancelWorkflow: MethodDefinition<_rota_v1_CancelWorkflowRequest, _rota_v1_CancelWorkflowResponse, _rota_v1_CancelWorkflowRequest__Output, _rota_v1_CancelWorkflowResponse__Output>
  GetWorkflowHistory: MethodDefinition<_rota_v1_WorkflowRunRef, _rota_v1_GetWorkflowHistoryResponse, _rota_v1_WorkflowRunRef__Output, _rota_v1_GetWorkflowHistoryResponse__Output>
  GetWorkflowRun: MethodDefinition<_rota_v1_WorkflowRunRef, _rota_v1_WorkflowRun, _rota_v1_WorkflowRunRef__Output, _rota_v1_WorkflowRun__Output>
  ListWorkflowRuns: MethodDefinition<_rota_v1_ListWorkflowRunsRequest, _rota_v1_ListWorkflowRunsResponse, _rota_v1_ListWorkflowRunsRequest__Output, _rota_v1_ListWorkflowRunsResponse__Output>
  PollActivityTask: MethodDefinition<_rota_v1_PollTaskRequest, _rota_v1_PolledActivityTask, _rota_v1_PollTaskRequest__Output, _rota_v1_PolledActivityTask__Output>
  PollWorkflowTask: MethodDefinition<_rota_v1_PollTaskRequest, _rota_v1_PolledWorkflowTask, _rota_v1_PollTaskRequest__Output, _rota_v1_PolledWorkflowTask__Output>
  RespondActivityTask: MethodDefinition<_rota_v1_RespondActivityTaskRequest, _rota_v1_RespondActivityTaskResponse, _rota_v1_RespondActivityTaskRequest__Output, _rota_v1_RespondActivityTaskResponse__Output>
  RespondWorkflowTask: MethodDefinition<_rota_v1_RespondWorkflowTaskRequest, _rota_v1_RespondWorkflowTaskResponse, _rota_v1_RespondWorkflowTaskRequest__Output, _rota_v1_RespondWorkflowTaskResponse__Output>
  SignalWorkflow: MethodDefinition<_rota_v1_SignalWorkflowRequest, _rota_v1_SignalWorkflowResponse, _rota_v1_SignalWorkflowRequest__Output, _rota_v1_SignalWorkflowResponse__Output>
  StartWorkflow: MethodDefinition<_rota_v1_StartWorkflowRequest, _rota_v1_StartWorkflowResponse, _rota_v1_StartWorkflowRequest__Output, _rota_v1_StartWorkflowResponse__Output>
}
