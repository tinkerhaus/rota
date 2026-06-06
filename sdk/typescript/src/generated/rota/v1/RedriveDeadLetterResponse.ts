// Original file: proto/rota/v1/rota.proto

import type { Long } from '@grpc/proto-loader';

export interface RedriveDeadLetterResponse {
  'ok'?: (boolean);
  'newMsgId'?: (number | string | Long);
}

export interface RedriveDeadLetterResponse__Output {
  'ok': (boolean);
  'newMsgId': (number);
}
