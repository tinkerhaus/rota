// Original file: proto/rota/v1/rota.proto

import type { Long } from '@grpc/proto-loader';

export interface RedriveDeadLetterRequest {
  'lane'?: (string);
  'groupId'?: (string);
  'msgId'?: (number | string | Long);
}

export interface RedriveDeadLetterRequest__Output {
  'lane': (string);
  'groupId': (string);
  'msgId': (number);
}
