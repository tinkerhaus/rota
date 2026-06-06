// Original file: proto/rota/v1/rota.proto

import type { Long } from '@grpc/proto-loader';

export interface GroupMeta {
  'lane'?: (string);
  'groupId'?: (string);
  'weight'?: (number | string);
  'batchSize'?: (number);
  'paused'?: (boolean);
  'deficit'?: (number | string);
  'virtualTime'?: (number | string);
  'readyCount'?: (number | string | Long);
  'inflightCount'?: (number | string | Long);
  'totalCount'?: (number | string | Long);
  'nextSeq'?: (number | string | Long);
  'lastServedSeq'?: (number | string | Long);
  'lastServedTs'?: (number | string | Long);
  'lastActivityMs'?: (number | string | Long);
  'delayedCount'?: (number | string | Long);
}

export interface GroupMeta__Output {
  'lane': (string);
  'groupId': (string);
  'weight': (number);
  'batchSize': (number);
  'paused': (boolean);
  'deficit': (number);
  'virtualTime': (number);
  'readyCount': (number);
  'inflightCount': (number);
  'totalCount': (number);
  'nextSeq': (number);
  'lastServedSeq': (number);
  'lastServedTs': (number);
  'lastActivityMs': (number);
  'delayedCount': (number);
}
