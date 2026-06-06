// Original file: proto/rota/v1/rota.proto

import type { Long } from '@grpc/proto-loader';

export interface WorkflowTaskRef {
  'runId'?: (number | string | Long);
  'workflowType'?: (string);
}

export interface WorkflowTaskRef__Output {
  'runId': (number);
  'workflowType': (string);
}
