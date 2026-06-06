// Original file: proto/rota/v1/rota.proto

import type { AuthAction as _rota_v1_AuthAction, AuthAction__Output as _rota_v1_AuthAction__Output } from '../../rota/v1/AuthAction';

export interface AuthGrant {
  'lanePattern'?: (string);
  'groupPattern'?: (string);
  'actions'?: (_rota_v1_AuthAction)[];
  'note'?: (string);
}

export interface AuthGrant__Output {
  'lanePattern': (string);
  'groupPattern': (string);
  'actions': (_rota_v1_AuthAction__Output)[];
  'note': (string);
}
