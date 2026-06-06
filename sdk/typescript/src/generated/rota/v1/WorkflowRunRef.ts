// Original file: proto/rota/v1/rota.proto

import type { Long } from '@grpc/proto-loader';

export interface WorkflowRunRef {
  'runId'?: (number | string | Long);
}

export interface WorkflowRunRef__Output {
  'runId': (number);
}
