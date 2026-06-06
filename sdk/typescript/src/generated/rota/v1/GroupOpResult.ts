// Original file: proto/rota/v1/rota.proto

import type { Long } from '@grpc/proto-loader';

export interface GroupOpResult {
  'affectedMessages'?: (number | string | Long);
}

export interface GroupOpResult__Output {
  'affectedMessages': (number);
}
