// Original file: proto/rota/v1/rota.proto

import type { Long } from '@grpc/proto-loader';

export interface ActivityCompletedAttrs {
  'scheduledEventId'?: (number | string | Long);
  'success'?: (boolean);
  'result'?: (Buffer | Uint8Array | string);
}

export interface ActivityCompletedAttrs__Output {
  'scheduledEventId': (number);
  'success': (boolean);
  'result': (Buffer);
}
