// Original file: proto/rota/v1/rota.proto

import type { Message as _rota_v1_Message, Message__Output as _rota_v1_Message__Output } from '../../rota/v1/Message';
import type { Long } from '@grpc/proto-loader';

export interface DeadLetter {
  'original'?: (_rota_v1_Message | null);
  'finalAttempt'?: (number);
  'reason'?: (string);
  'failureHeaders'?: ({[key: string]: string});
  'deadAtMs'?: (number | string | Long);
}

export interface DeadLetter__Output {
  'original': (_rota_v1_Message__Output | null);
  'finalAttempt': (number);
  'reason': (string);
  'failureHeaders': ({[key: string]: string});
  'deadAtMs': (number);
}
