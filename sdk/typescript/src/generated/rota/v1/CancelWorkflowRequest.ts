// Original file: proto/rota/v1/rota.proto

import type { Long } from '@grpc/proto-loader';

export interface CancelWorkflowRequest {
  'runId'?: (number | string | Long);
  'reason'?: (Buffer | Uint8Array | string);
}

export interface CancelWorkflowRequest__Output {
  'runId': (number);
  'reason': (Buffer);
}
