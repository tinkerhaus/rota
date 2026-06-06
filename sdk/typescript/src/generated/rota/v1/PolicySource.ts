// Original file: proto/rota/v1/rota.proto

import type { PolicyKind as _rota_v1_PolicyKind, PolicyKind__Output as _rota_v1_PolicyKind__Output } from '../../rota/v1/PolicyKind';
import type { PolicyMode as _rota_v1_PolicyMode, PolicyMode__Output as _rota_v1_PolicyMode__Output } from '../../rota/v1/PolicyMode';

export interface PolicySource {
  'kind'?: (_rota_v1_PolicyKind);
  'mode'?: (_rota_v1_PolicyMode);
  'engine'?: (string);
  'code'?: (Buffer | Uint8Array | string);
  'params'?: ({[key: string]: string});
}

export interface PolicySource__Output {
  'kind': (_rota_v1_PolicyKind__Output);
  'mode': (_rota_v1_PolicyMode__Output);
  'engine': (string);
  'code': (Buffer);
  'params': ({[key: string]: string});
}
