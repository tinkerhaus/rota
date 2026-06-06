// Original file: proto/rota/v1/rota.proto

import type { Long } from '@grpc/proto-loader';

export interface TeardownResult {
  'affectedLanes'?: (string)[];
  'affectedMessages'?: (number | string | Long);
}

export interface TeardownResult__Output {
  'affectedLanes': (string)[];
  'affectedMessages': (number);
}
