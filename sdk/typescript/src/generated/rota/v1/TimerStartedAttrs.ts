// Original file: proto/rota/v1/rota.proto

import type { Long } from '@grpc/proto-loader';

export interface TimerStartedAttrs {
  'fireAtMs'?: (number | string | Long);
}

export interface TimerStartedAttrs__Output {
  'fireAtMs': (number);
}
