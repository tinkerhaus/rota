// Original file: proto/rota/v1/rota.proto

import type { Long } from '@grpc/proto-loader';

export interface ReleaseSingletonRequest {
  'name'?: (string);
  'holder'?: (string);
  'fence'?: (number | string | Long);
}

export interface ReleaseSingletonRequest__Output {
  'name': (string);
  'holder': (string);
  'fence': (number);
}
