// Original file: proto/rota/v1/rota.proto

import type { WorkflowCommandProto as _rota_v1_WorkflowCommandProto, WorkflowCommandProto__Output as _rota_v1_WorkflowCommandProto__Output } from '../../rota/v1/WorkflowCommandProto';
import type { Long } from '@grpc/proto-loader';

export interface RespondWorkflowTaskRequest {
  'runId'?: (number | string | Long);
  'leaseId'?: (number | string | Long);
  'runEpoch'?: (number);
  'historySeq'?: (number | string | Long);
  'prefixChecksum'?: (Buffer | Uint8Array | string);
  'commands'?: (_rota_v1_WorkflowCommandProto)[];
}

export interface RespondWorkflowTaskRequest__Output {
  'runId': (number);
  'leaseId': (number);
  'runEpoch': (number);
  'historySeq': (number);
  'prefixChecksum': (Buffer);
  'commands': (_rota_v1_WorkflowCommandProto__Output)[];
}
