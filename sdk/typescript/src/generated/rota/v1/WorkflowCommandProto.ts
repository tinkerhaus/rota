// Original file: proto/rota/v1/rota.proto

import type { Long } from '@grpc/proto-loader';

export interface WorkflowCommandProto {
  'kind'?: (string);
  'activityType'?: (string);
  'input'?: (Buffer | Uint8Array | string);
  'result'?: (Buffer | Uint8Array | string);
  'delayMs'?: (number | string | Long);
}

export interface WorkflowCommandProto__Output {
  'kind': (string);
  'activityType': (string);
  'input': (Buffer);
  'result': (Buffer);
  'delayMs': (number);
}
