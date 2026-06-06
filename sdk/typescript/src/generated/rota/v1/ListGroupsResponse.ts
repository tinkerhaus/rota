// Original file: proto/rota/v1/rota.proto

import type { GroupStats as _rota_v1_GroupStats, GroupStats__Output as _rota_v1_GroupStats__Output } from '../../rota/v1/GroupStats';

export interface ListGroupsResponse {
  'groups'?: (_rota_v1_GroupStats)[];
  'nextPageToken'?: (string);
}

export interface ListGroupsResponse__Output {
  'groups': (_rota_v1_GroupStats__Output)[];
  'nextPageToken': (string);
}
