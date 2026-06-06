// Original file: proto/rota/v1/rota.proto

import type { Long } from '@grpc/proto-loader';

export interface TimerFiredAttrs {
  'startedEventId'?: (number | string | Long);
}

export interface TimerFiredAttrs__Output {
  'startedEventId': (number);
}
