// Original file: proto/rota/v1/rota.proto

import type { PolicySource as _rota_v1_PolicySource, PolicySource__Output as _rota_v1_PolicySource__Output } from '../../rota/v1/PolicySource';
import type { Long } from '@grpc/proto-loader';

export interface PolicyInfo {
  'lane'?: (string);
  'source'?: (_rota_v1_PolicySource | null);
  'policyVersion'?: (number | string | Long);
  'sourceHash'?: (string);
  'faultCount'?: (number | string | Long);
  'quarantined'?: (boolean);
}

export interface PolicyInfo__Output {
  'lane': (string);
  'source': (_rota_v1_PolicySource__Output | null);
  'policyVersion': (number);
  'sourceHash': (string);
  'faultCount': (number);
  'quarantined': (boolean);
}
