// Original file: proto/rota/v1/rota.proto

import type { AuthGrant as _rota_v1_AuthGrant, AuthGrant__Output as _rota_v1_AuthGrant__Output } from '../../rota/v1/AuthGrant';

export interface CreatePrincipalRequest {
  'name'?: (string);
  'tags'?: (string)[];
  'grants'?: (_rota_v1_AuthGrant)[];
}

export interface CreatePrincipalRequest__Output {
  'name': (string);
  'tags': (string)[];
  'grants': (_rota_v1_AuthGrant__Output)[];
}
