// Original file: proto/rota/v1/rota.proto

import type { AuthPrincipal as _rota_v1_AuthPrincipal, AuthPrincipal__Output as _rota_v1_AuthPrincipal__Output } from '../../rota/v1/AuthPrincipal';

export interface RotatePrincipalTokenResponse {
  'principal'?: (_rota_v1_AuthPrincipal | null);
  'token'?: (string);
}

export interface RotatePrincipalTokenResponse__Output {
  'principal': (_rota_v1_AuthPrincipal__Output | null);
  'token': (string);
}
