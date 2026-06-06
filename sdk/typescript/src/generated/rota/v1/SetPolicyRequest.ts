// Original file: proto/rota/v1/rota.proto

import type { PolicySource as _rota_v1_PolicySource, PolicySource__Output as _rota_v1_PolicySource__Output } from '../../rota/v1/PolicySource';

export interface SetPolicyRequest {
  'lane'?: (string);
  'source'?: (_rota_v1_PolicySource | null);
}

export interface SetPolicyRequest__Output {
  'lane': (string);
  'source': (_rota_v1_PolicySource__Output | null);
}
