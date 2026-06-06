// Original file: proto/rota/v1/rota.proto

import type { Duration as _google_protobuf_Duration, Duration__Output as _google_protobuf_Duration__Output } from '../../google/protobuf/Duration';
import type { Long } from '@grpc/proto-loader';

export interface RenewSingletonRequest {
  'name'?: (string);
  'holder'?: (string);
  'fence'?: (number | string | Long);
  'ttl'?: (_google_protobuf_Duration | null);
}

export interface RenewSingletonRequest__Output {
  'name': (string);
  'holder': (string);
  'fence': (number);
  'ttl': (_google_protobuf_Duration__Output | null);
}
