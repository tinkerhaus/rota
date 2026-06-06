// Original file: proto/rota/v1/rota.proto

import type { Long } from '@grpc/proto-loader';

export interface SingletonLease {
  'name'?: (string);
  'holder'?: (string);
  'deadlineMs'?: (number | string | Long);
  'fence'?: (number | string | Long);
}

export interface SingletonLease__Output {
  'name': (string);
  'holder': (string);
  'deadlineMs': (number);
  'fence': (number);
}
