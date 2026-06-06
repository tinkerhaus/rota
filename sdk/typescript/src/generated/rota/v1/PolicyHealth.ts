// Original file: proto/rota/v1/rota.proto

import type { Long } from '@grpc/proto-loader';

export interface PolicyHealth {
  'lane'?: (string);
  'version'?: (number | string | Long);
  'quarantined'?: (boolean);
  'engine'?: (string);
  'faults'?: (number | string | Long);
}

export interface PolicyHealth__Output {
  'lane': (string);
  'version': (number);
  'quarantined': (boolean);
  'engine': (string);
  'faults': (number);
}
