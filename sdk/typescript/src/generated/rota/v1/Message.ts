// Original file: proto/rota/v1/rota.proto

import type { MessageState as _rota_v1_MessageState, MessageState__Output as _rota_v1_MessageState__Output } from '../../rota/v1/MessageState';
import type { Long } from '@grpc/proto-loader';

export interface Message {
  'msgId'?: (number | string | Long);
  'lane'?: (string);
  'groupId'?: (string);
  'payload'?: (Buffer | Uint8Array | string);
  'headers'?: ({[key: string]: string});
  'notBeforeMs'?: (number | string | Long);
  'ttlMs'?: (number | string | Long);
  'attempt'?: (number);
  'maxAttempts'?: (number);
  'enqueueMs'?: (number | string | Long);
  'state'?: (_rota_v1_MessageState);
  'epoch'?: (number);
  'curLease'?: (number | string | Long);
  'issueToken'?: (boolean);
  'externalToken'?: (Buffer | Uint8Array | string);
}

export interface Message__Output {
  'msgId': (number);
  'lane': (string);
  'groupId': (string);
  'payload': (Buffer);
  'headers': ({[key: string]: string});
  'notBeforeMs': (number);
  'ttlMs': (number);
  'attempt': (number);
  'maxAttempts': (number);
  'enqueueMs': (number);
  'state': (_rota_v1_MessageState__Output);
  'epoch': (number);
  'curLease': (number);
  'issueToken': (boolean);
  'externalToken': (Buffer);
}
