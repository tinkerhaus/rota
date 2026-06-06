// Original file: proto/rota/v1/rota.proto

import type { Duration as _google_protobuf_Duration, Duration__Output as _google_protobuf_Duration__Output } from '../../google/protobuf/Duration';

export interface RetryBackoff {
  'base'?: (_google_protobuf_Duration | null);
  'multiplier'?: (number | string);
  'max'?: (_google_protobuf_Duration | null);
}

export interface RetryBackoff__Output {
  'base': (_google_protobuf_Duration__Output | null);
  'multiplier': (number);
  'max': (_google_protobuf_Duration__Output | null);
}
