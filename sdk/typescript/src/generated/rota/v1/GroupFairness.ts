// Original file: proto/rota/v1/rota.proto

import type { Long } from '@grpc/proto-loader';

export interface GroupFairness {
  'groupId'?: (string);
  'weight'?: (number | string);
  'expectedShare'?: (number | string);
  'actualShare'?: (number | string);
  'virtualTime'?: (number | string);
  'deficit'?: (number | string);
  'served'?: (number | string | Long);
  'starvationScore'?: (number | string);
  'paused'?: (boolean);
}

export interface GroupFairness__Output {
  'groupId': (string);
  'weight': (number);
  'expectedShare': (number);
  'actualShare': (number);
  'virtualTime': (number);
  'deficit': (number);
  'served': (number);
  'starvationScore': (number);
  'paused': (boolean);
}
