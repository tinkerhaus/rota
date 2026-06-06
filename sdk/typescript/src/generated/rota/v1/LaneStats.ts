// Original file: proto/rota/v1/rota.proto

import type { Long } from '@grpc/proto-loader';

export interface LaneStats {
  'lane'?: (string);
  'leasable'?: (number | string | Long);
  'delayed'?: (number | string | Long);
  'inflight'?: (number | string | Long);
  'dlqDepth'?: (number | string | Long);
  'publishRate'?: (number | string);
  'leaseRate'?: (number | string);
  'ackRate'?: (number | string);
  'oldestAgeMs'?: (number | string | Long);
  'groupCount'?: (number | string | Long);
  'policyVersion'?: (number | string | Long);
}

export interface LaneStats__Output {
  'lane': (string);
  'leasable': (number);
  'delayed': (number);
  'inflight': (number);
  'dlqDepth': (number);
  'publishRate': (number);
  'leaseRate': (number);
  'ackRate': (number);
  'oldestAgeMs': (number);
  'groupCount': (number);
  'policyVersion': (number);
}
