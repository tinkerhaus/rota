// Original file: proto/rota/v1/rota.proto

import type { WorkflowRun as _rota_v1_WorkflowRun, WorkflowRun__Output as _rota_v1_WorkflowRun__Output } from '../../rota/v1/WorkflowRun';

export interface ListWorkflowRunsResponse {
  'runs'?: (_rota_v1_WorkflowRun)[];
  'nextPageToken'?: (string);
}

export interface ListWorkflowRunsResponse__Output {
  'runs': (_rota_v1_WorkflowRun__Output)[];
  'nextPageToken': (string);
}
