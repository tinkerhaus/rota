// Original file: proto/rota/v1/rota.proto

import type { Long } from '@grpc/proto-loader';

export interface RespondActivityTaskRequest {
  'runId'?: (number | string | Long);
  'leaseId'?: (number | string | Long);
  'scheduledEventId'?: (number | string | Long);
  'success'?: (boolean);
  'result'?: (Buffer | Uint8Array | string);
}

export interface RespondActivityTaskRequest__Output {
  'runId': (number);
  'leaseId': (number);
  'scheduledEventId': (number);
  'success': (boolean);
  'result': (Buffer);
}
