// Original file: proto/rota/v1/rota.proto

import type { WorkflowStatus as _rota_v1_WorkflowStatus, WorkflowStatus__Output as _rota_v1_WorkflowStatus__Output } from '../../rota/v1/WorkflowStatus';
import type { Long } from '@grpc/proto-loader';

export interface WorkflowRun {
  'runId'?: (number | string | Long);
  'workflowType'?: (string);
  'tenantId'?: (string);
  'status'?: (_rota_v1_WorkflowStatus);
  'runEpoch'?: (number);
  'curHistorySeq'?: (number | string | Long);
  'input'?: (Buffer | Uint8Array | string);
  'startedMs'?: (number | string | Long);
  'lastEventMs'?: (number | string | Long);
  'parentRunId'?: (number | string | Long);
  'wfTaskPending'?: (boolean);
}

export interface WorkflowRun__Output {
  'runId': (number);
  'workflowType': (string);
  'tenantId': (string);
  'status': (_rota_v1_WorkflowStatus__Output);
  'runEpoch': (number);
  'curHistorySeq': (number);
  'input': (Buffer);
  'startedMs': (number);
  'lastEventMs': (number);
  'parentRunId': (number);
  'wfTaskPending': (boolean);
}
