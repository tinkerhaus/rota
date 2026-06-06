// Original file: proto/rota/v1/rota.proto

import type { Long } from '@grpc/proto-loader';

export interface AuthTokenInfo {
  'tokenId'?: (string);
  'salt'?: (Buffer | Uint8Array | string);
  'hash'?: (Buffer | Uint8Array | string);
  'createdMs'?: (number | string | Long);
  'rotatedMs'?: (number | string | Long);
}

export interface AuthTokenInfo__Output {
  'tokenId': (string);
  'salt': (Buffer);
  'hash': (Buffer);
  'createdMs': (number);
  'rotatedMs': (number);
}
