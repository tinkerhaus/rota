// Original file: proto/rota/v1/rota.proto

import type { AuthGrant as _rota_v1_AuthGrant, AuthGrant__Output as _rota_v1_AuthGrant__Output } from '../../rota/v1/AuthGrant';
import type { AuthTokenInfo as _rota_v1_AuthTokenInfo, AuthTokenInfo__Output as _rota_v1_AuthTokenInfo__Output } from '../../rota/v1/AuthTokenInfo';
import type { Long } from '@grpc/proto-loader';

export interface AuthPrincipal {
  'name'?: (string);
  'disabled'?: (boolean);
  'tags'?: (string)[];
  'grants'?: (_rota_v1_AuthGrant)[];
  'token'?: (_rota_v1_AuthTokenInfo | null);
  'createdMs'?: (number | string | Long);
  'updatedMs'?: (number | string | Long);
}

export interface AuthPrincipal__Output {
  'name': (string);
  'disabled': (boolean);
  'tags': (string)[];
  'grants': (_rota_v1_AuthGrant__Output)[];
  'token': (_rota_v1_AuthTokenInfo__Output | null);
  'createdMs': (number);
  'updatedMs': (number);
}
