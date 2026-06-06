// Original file: proto/rota/v1/rota.proto

import type { Long } from '@grpc/proto-loader';

export interface GroupStats {
  'lane'?: (string);
  'groupId'?: (string);
  'weight'?: (number | string);
  'paused'?: (boolean);
  'ready'?: (number | string | Long);
  'delayed'?: (number | string | Long);
  'inflight'?: (number | string | Long);
  'total'?: (number | string | Long);
  'virtualTime'?: (number | string);
  'deficit'?: (number | string);
  'lastActivityMs'?: (number | string | Long);
}

export interface GroupStats__Output {
  'lane': (string);
  'groupId': (string);
  'weight': (number);
  'paused': (boolean);
  'ready': (number);
  'delayed': (number);
  'inflight': (number);
  'total': (number);
  'virtualTime': (number);
  'deficit': (number);
  'lastActivityMs': (number);
}
