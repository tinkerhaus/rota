// Original file: proto/rota/v1/rota.proto

import type { Timestamp as _google_protobuf_Timestamp, Timestamp__Output as _google_protobuf_Timestamp__Output } from '../../google/protobuf/Timestamp';
import type { Long } from '@grpc/proto-loader';

export interface LeasedMessage {
  'leaseId'?: (number | string | Long);
  'lane'?: (string);
  'groupId'?: (string);
  'payload'?: (Buffer | Uint8Array | string);
  'attempt'?: (number);
  'headers'?: ({[key: string]: string});
  'visibilityDeadline'?: (_google_protobuf_Timestamp | null);
  'externalToken'?: (Buffer | Uint8Array | string);
  'messageId'?: (number | string | Long);
}

export interface LeasedMessage__Output {
  'leaseId': (number);
  'lane': (string);
  'groupId': (string);
  'payload': (Buffer);
  'attempt': (number);
  'headers': ({[key: string]: string});
  'visibilityDeadline': (_google_protobuf_Timestamp__Output | null);
  'externalToken': (Buffer);
  'messageId': (number);
}
