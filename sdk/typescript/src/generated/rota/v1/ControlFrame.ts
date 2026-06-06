// Original file: proto/rota/v1/rota.proto

import type { ControlKind as _rota_v1_ControlKind, ControlKind__Output as _rota_v1_ControlKind__Output } from '../../rota/v1/ControlKind';

export interface ControlFrame {
  'kind'?: (_rota_v1_ControlKind);
  'lane'?: (string);
  'groupId'?: (string);
}

export interface ControlFrame__Output {
  'kind': (_rota_v1_ControlKind__Output);
  'lane': (string);
  'groupId': (string);
}
