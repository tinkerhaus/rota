// Original file: proto/rota/v1/rota.proto

import type { NackMode as _rota_v1_NackMode, NackMode__Output as _rota_v1_NackMode__Output } from '../../rota/v1/NackMode';
import type { Duration as _google_protobuf_Duration, Duration__Output as _google_protobuf_Duration__Output } from '../../google/protobuf/Duration';
import type { Long } from '@grpc/proto-loader';

export interface Nack {
  'leaseId'?: (number | string | Long);
  'mode'?: (_rota_v1_NackMode);
  'delay'?: (_google_protobuf_Duration | null);
  'failureMeta'?: ({[key: string]: string});
}

export interface Nack__Output {
  'leaseId': (number);
  'mode': (_rota_v1_NackMode__Output);
  'delay': (_google_protobuf_Duration__Output | null);
  'failureMeta': ({[key: string]: string});
}
