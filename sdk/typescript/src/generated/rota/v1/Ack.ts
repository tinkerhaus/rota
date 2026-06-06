// Original file: proto/rota/v1/rota.proto

import type { Long } from '@grpc/proto-loader';

export interface Ack {
  'leaseId'?: (number | string | Long);
}

export interface Ack__Output {
  'leaseId': (number);
}
