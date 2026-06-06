// Original file: proto/rota/v1/rota.proto

import type { Long } from '@grpc/proto-loader';

export interface SignalWorkflowRequest {
  'runId'?: (number | string | Long);
  'signalName'?: (string);
  'payload'?: (Buffer | Uint8Array | string);
}

export interface SignalWorkflowRequest__Output {
  'runId': (number);
  'signalName': (string);
  'payload': (Buffer);
}
