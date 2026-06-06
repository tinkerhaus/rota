// Original file: proto/rota/v1/rota.proto

import type { Long } from '@grpc/proto-loader';

export interface CronInfo {
  'cronId'?: (string);
  'lane'?: (string);
  'schedule'?: (string);
  'nextFireMs'?: (number | string | Long);
  'lastFireMs'?: (number | string | Long);
  'paused'?: (boolean);
}

export interface CronInfo__Output {
  'cronId': (string);
  'lane': (string);
  'schedule': (string);
  'nextFireMs': (number);
  'lastFireMs': (number);
  'paused': (boolean);
}
