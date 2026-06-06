// Original file: proto/rota/v1/rota.proto

import type { ErrorCode as _rota_v1_ErrorCode, ErrorCode__Output as _rota_v1_ErrorCode__Output } from '../../rota/v1/ErrorCode';
import type { Long } from '@grpc/proto-loader';

export interface PublishItemResult {
  'messageId'?: (number | string | Long);
  'code'?: (_rota_v1_ErrorCode);
  'detail'?: (string);
}

export interface PublishItemResult__Output {
  'messageId': (number);
  'code': (_rota_v1_ErrorCode__Output);
  'detail': (string);
}
