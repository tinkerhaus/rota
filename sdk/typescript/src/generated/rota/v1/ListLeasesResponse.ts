// Original file: proto/rota/v1/rota.proto

import type { LeaseInfo as _rota_v1_LeaseInfo, LeaseInfo__Output as _rota_v1_LeaseInfo__Output } from '../../rota/v1/LeaseInfo';

export interface ListLeasesResponse {
  'leases'?: (_rota_v1_LeaseInfo)[];
  'nextPageToken'?: (string);
}

export interface ListLeasesResponse__Output {
  'leases': (_rota_v1_LeaseInfo__Output)[];
  'nextPageToken': (string);
}
