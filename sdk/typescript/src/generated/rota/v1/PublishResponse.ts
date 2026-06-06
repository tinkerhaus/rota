// Original file: proto/rota/v1/rota.proto

import type { Long } from '@grpc/proto-loader';

export interface PublishResponse {
  'messageId'?: (number | string | Long);
}

export interface PublishResponse__Output {
  'messageId': (number);
}
