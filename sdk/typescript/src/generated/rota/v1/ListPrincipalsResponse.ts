// Original file: proto/rota/v1/rota.proto

import type { AuthPrincipal as _rota_v1_AuthPrincipal, AuthPrincipal__Output as _rota_v1_AuthPrincipal__Output } from '../../rota/v1/AuthPrincipal';

export interface ListPrincipalsResponse {
  'authEnabled'?: (boolean);
  'principals'?: (_rota_v1_AuthPrincipal)[];
}

export interface ListPrincipalsResponse__Output {
  'authEnabled': (boolean);
  'principals': (_rota_v1_AuthPrincipal__Output)[];
}
