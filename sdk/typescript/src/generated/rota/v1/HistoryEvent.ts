// Original file: proto/rota/v1/rota.proto

import type { HistoryEventType as _rota_v1_HistoryEventType, HistoryEventType__Output as _rota_v1_HistoryEventType__Output } from '../../rota/v1/HistoryEventType';
import type { Long } from '@grpc/proto-loader';

export interface HistoryEvent {
  'eventId'?: (number | string | Long);
  'eventType'?: (_rota_v1_HistoryEventType);
  'eventTimeMs'?: (number | string | Long);
  'attrs'?: (Buffer | Uint8Array | string);
}

export interface HistoryEvent__Output {
  'eventId': (number);
  'eventType': (_rota_v1_HistoryEventType__Output);
  'eventTimeMs': (number);
  'attrs': (Buffer);
}
