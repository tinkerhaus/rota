// Original file: proto/rota/v1/rota.proto

import type { Long } from '@grpc/proto-loader';

export interface PolledActivityTask {
  'empty'?: (boolean);
  'runId'?: (number | string | Long);
  'leaseId'?: (number | string | Long);
  'scheduledEventId'?: (number | string | Long);
  'activityType'?: (string);
  'input'?: (Buffer | Uint8Array | string);
}

export interface PolledActivityTask__Output {
  'empty': (boolean);
  'runId': (number);
  'leaseId': (number);
  'scheduledEventId': (number);
  'activityType': (string);
  'input': (Buffer);
}
