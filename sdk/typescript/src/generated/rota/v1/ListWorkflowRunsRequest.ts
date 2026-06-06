// Original file: proto/rota/v1/rota.proto

import type { WorkflowStatus as _rota_v1_WorkflowStatus, WorkflowStatus__Output as _rota_v1_WorkflowStatus__Output } from '../../rota/v1/WorkflowStatus';

export interface ListWorkflowRunsRequest {
  'status'?: (_rota_v1_WorkflowStatus);
  'hasStatus'?: (boolean);
  'pageSize'?: (number);
  'pageToken'?: (string);
}

export interface ListWorkflowRunsRequest__Output {
  'status': (_rota_v1_WorkflowStatus__Output);
  'hasStatus': (boolean);
  'pageSize': (number);
  'pageToken': (string);
}
