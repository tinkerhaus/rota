// Original file: proto/rota/v1/rota.proto

import type { Long } from '@grpc/proto-loader';

export interface ActivityTask {
  'runId'?: (number | string | Long);
  'scheduledEventId'?: (number | string | Long);
  'activityType'?: (string);
  'input'?: (Buffer | Uint8Array | string);
}

export interface ActivityTask__Output {
  'runId': (number);
  'scheduledEventId': (number);
  'activityType': (string);
  'input': (Buffer);
}
