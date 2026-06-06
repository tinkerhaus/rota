// Original file: proto/rota/v1/rota.proto

import type { Long } from '@grpc/proto-loader';

export interface DeadLetterInfo {
  'lane'?: (string);
  'groupId'?: (string);
  'msgId'?: (number | string | Long);
  'finalAttempt'?: (number);
  'reason'?: (string);
  'deadAtMs'?: (number | string | Long);
  'failureHeaders'?: ({[key: string]: string});
  'headers'?: ({[key: string]: string});
  'payload'?: (Buffer | Uint8Array | string);
}

export interface DeadLetterInfo__Output {
  'lane': (string);
  'groupId': (string);
  'msgId': (number);
  'finalAttempt': (number);
  'reason': (string);
  'deadAtMs': (number);
  'failureHeaders': ({[key: string]: string});
  'headers': ({[key: string]: string});
  'payload': (Buffer);
}
