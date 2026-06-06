// Original file: proto/rota/v1/rota.proto

import type { GroupFairness as _rota_v1_GroupFairness, GroupFairness__Output as _rota_v1_GroupFairness__Output } from '../../rota/v1/GroupFairness';
import type { Long } from '@grpc/proto-loader';

export interface LaneFairness {
  'lane'?: (string);
  'groups'?: (_rota_v1_GroupFairness)[];
  'totalServed'?: (number | string | Long);
}

export interface LaneFairness__Output {
  'lane': (string);
  'groups': (_rota_v1_GroupFairness__Output)[];
  'totalServed': (number);
}
