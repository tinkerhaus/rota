// Original file: proto/rota/v1/rota.proto

import type { ErrorCode as _rota_v1_ErrorCode, ErrorCode__Output as _rota_v1_ErrorCode__Output } from '../../rota/v1/ErrorCode';
import type { Long } from '@grpc/proto-loader';

export interface StreamError {
  'code'?: (_rota_v1_ErrorCode);
  'detail'?: (string);
  'leaseId'?: (number | string | Long);
  'leaderAddr'?: (string);
}

export interface StreamError__Output {
  'code': (_rota_v1_ErrorCode__Output);
  'detail': (string);
  'leaseId': (number);
  'leaderAddr': (string);
}
