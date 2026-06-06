// Original file: proto/rota/v1/rota.proto

import type { MisfirePolicy as _rota_v1_MisfirePolicy, MisfirePolicy__Output as _rota_v1_MisfirePolicy__Output } from '../../rota/v1/MisfirePolicy';
import type { Timestamp as _google_protobuf_Timestamp, Timestamp__Output as _google_protobuf_Timestamp__Output } from '../../google/protobuf/Timestamp';

export interface ScheduleCronRequest {
  'cronId'?: (string);
  'lane'?: (string);
  'groupId'?: (string);
  'payload'?: (Buffer | Uint8Array | string);
  'headers'?: ({[key: string]: string});
  'schedule'?: (string);
  'timezone'?: (string);
  'misfire'?: (_rota_v1_MisfirePolicy);
  'coalesce'?: (boolean);
  'startAt'?: (_google_protobuf_Timestamp | null);
  'endAt'?: (_google_protobuf_Timestamp | null);
}

export interface ScheduleCronRequest__Output {
  'cronId': (string);
  'lane': (string);
  'groupId': (string);
  'payload': (Buffer);
  'headers': ({[key: string]: string});
  'schedule': (string);
  'timezone': (string);
  'misfire': (_rota_v1_MisfirePolicy__Output);
  'coalesce': (boolean);
  'startAt': (_google_protobuf_Timestamp__Output | null);
  'endAt': (_google_protobuf_Timestamp__Output | null);
}
