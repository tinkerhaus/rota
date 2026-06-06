// Original file: proto/rota/v1/rota.proto

import type { PeerInfo as _rota_v1_PeerInfo, PeerInfo__Output as _rota_v1_PeerInfo__Output } from '../../rota/v1/PeerInfo';
import type { Long } from '@grpc/proto-loader';

export interface ClusterInfo {
  'leaderId'?: (string);
  'leaderAddr'?: (string);
  'term'?: (number | string | Long);
  'appliedIndex'?: (number | string | Long);
  'peers'?: (_rota_v1_PeerInfo)[];
}

export interface ClusterInfo__Output {
  'leaderId': (string);
  'leaderAddr': (string);
  'term': (number);
  'appliedIndex': (number);
  'peers': (_rota_v1_PeerInfo__Output)[];
}
