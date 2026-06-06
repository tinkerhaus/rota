// Original file: proto/rota/v1/rota.proto

import type { Outcome as _rota_v1_Outcome, Outcome__Output as _rota_v1_Outcome__Output } from '../../rota/v1/Outcome';
import type { Duration as _google_protobuf_Duration, Duration__Output as _google_protobuf_Duration__Output } from '../../google/protobuf/Duration';

export interface Complete {
  'externalToken'?: (Buffer | Uint8Array | string);
  'outcome'?: (_rota_v1_Outcome);
  'resultMeta'?: ({[key: string]: string});
  'delay'?: (_google_protobuf_Duration | null);
}

export interface Complete__Output {
  'externalToken': (Buffer);
  'outcome': (_rota_v1_Outcome__Output);
  'resultMeta': ({[key: string]: string});
  'delay': (_google_protobuf_Duration__Output | null);
}
