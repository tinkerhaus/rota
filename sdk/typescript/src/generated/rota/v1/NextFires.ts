// Original file: proto/rota/v1/rota.proto

import type { Long } from '@grpc/proto-loader';

export interface NextFires {
  'fireMs'?: (number | string | Long)[];
}

export interface NextFires__Output {
  'fireMs': (number)[];
}
