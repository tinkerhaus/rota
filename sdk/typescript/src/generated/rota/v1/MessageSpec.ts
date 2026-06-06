// Original file: proto/rota/v1/rota.proto

import type { Duration as _google_protobuf_Duration, Duration__Output as _google_protobuf_Duration__Output } from '../../google/protobuf/Duration';
import type { Timestamp as _google_protobuf_Timestamp, Timestamp__Output as _google_protobuf_Timestamp__Output } from '../../google/protobuf/Timestamp';
import type { RetryBackoff as _rota_v1_RetryBackoff, RetryBackoff__Output as _rota_v1_RetryBackoff__Output } from '../../rota/v1/RetryBackoff';

export interface MessageSpec {
  'lane'?: (string);
  'groupId'?: (string);
  'payload'?: (Buffer | Uint8Array | string);
  'headers'?: ({[key: string]: string});
  'delay'?: (_google_protobuf_Duration | null);
  'at'?: (_google_protobuf_Timestamp | null);
  'maxAttempts'?: (number);
  'retryBackoff'?: (_rota_v1_RetryBackoff | null);
  'ttl'?: (_google_protobuf_Duration | null);
  'weight'?: (number | string);
  'batchSize'?: (number);
  'dedupKey'?: (string);
  'issueToken'?: (boolean);
  'externalToken'?: (Buffer | Uint8Array | string);
  'notBefore'?: "delay"|"at";
  '_weight'?: "weight";
  '_batchSize'?: "batchSize";
}

export interface MessageSpec__Output {
  'lane': (string);
  'groupId': (string);
  'payload': (Buffer);
  'headers': ({[key: string]: string});
  'delay'?: (_google_protobuf_Duration__Output | null);
  'at'?: (_google_protobuf_Timestamp__Output | null);
  'maxAttempts': (number);
  'retryBackoff': (_rota_v1_RetryBackoff__Output | null);
  'ttl': (_google_protobuf_Duration__Output | null);
  'weight'?: (number);
  'batchSize'?: (number);
  'dedupKey': (string);
  'issueToken': (boolean);
  'externalToken': (Buffer);
  'notBefore'?: "delay"|"at";
  '_weight'?: "weight";
  '_batchSize'?: "batchSize";
}
