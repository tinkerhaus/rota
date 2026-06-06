// Original file: proto/rota/v1/rota.proto

import type { Duration as _google_protobuf_Duration, Duration__Output as _google_protobuf_Duration__Output } from '../../google/protobuf/Duration';

export interface AcquireSingletonRequest {
  'name'?: (string);
  'holder'?: (string);
  'ttl'?: (_google_protobuf_Duration | null);
}

export interface AcquireSingletonRequest__Output {
  'name': (string);
  'holder': (string);
  'ttl': (_google_protobuf_Duration__Output | null);
}
