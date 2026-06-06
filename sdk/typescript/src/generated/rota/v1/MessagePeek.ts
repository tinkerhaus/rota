// Original file: proto/rota/v1/rota.proto

import type { MessageState as _rota_v1_MessageState, MessageState__Output as _rota_v1_MessageState__Output } from '../../rota/v1/MessageState';
import type { Long } from '@grpc/proto-loader';

export interface MessagePeek {
  'msgId'?: (number | string | Long);
  'state'?: (_rota_v1_MessageState);
  'attempt'?: (number);
  'enqueueMs'?: (number | string | Long);
  'notBeforeMs'?: (number | string | Long);
}

export interface MessagePeek__Output {
  'msgId': (number);
  'state': (_rota_v1_MessageState__Output);
  'attempt': (number);
  'enqueueMs': (number);
  'notBeforeMs': (number);
}
