// Original file: proto/rota/v1/rota.proto

import type { AuthGrant as _rota_v1_AuthGrant, AuthGrant__Output as _rota_v1_AuthGrant__Output } from '../../rota/v1/AuthGrant';

export interface GrantPrincipalRequest {
  'name'?: (string);
  'grant'?: (_rota_v1_AuthGrant | null);
}

export interface GrantPrincipalRequest__Output {
  'name': (string);
  'grant': (_rota_v1_AuthGrant__Output | null);
}
