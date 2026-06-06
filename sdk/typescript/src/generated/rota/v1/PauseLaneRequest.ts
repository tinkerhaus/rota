// Original file: proto/rota/v1/rota.proto

import type { Duration as _google_protobuf_Duration, Duration__Output as _google_protobuf_Duration__Output } from '../../google/protobuf/Duration';

export interface PauseLaneRequest {
  'lane'?: (string);
  'duration'?: (_google_protobuf_Duration | null);
}

export interface PauseLaneRequest__Output {
  'lane': (string);
  'duration': (_google_protobuf_Duration__Output | null);
}
