// Original file: proto/rota/v1/rota.proto

import type { Long } from '@grpc/proto-loader';

export interface Lease {
  'leaseId'?: (number | string | Long);
  'msgId'?: (number | string | Long);
  'lane'?: (string);
  'groupId'?: (string);
  'consumerId'?: (string);
  'deadlineMs'?: (number | string | Long);
  'attemptAtLease'?: (number);
  'epoch'?: (number);
  'extendCount'?: (number);
  'completionTokenHash'?: (Buffer | Uint8Array | string);
  'grantedMs'?: (number | string | Long);
}

export interface Lease__Output {
  'leaseId': (number);
  'msgId': (number);
  'lane': (string);
  'groupId': (string);
  'consumerId': (string);
  'deadlineMs': (number);
  'attemptAtLease': (number);
  'epoch': (number);
  'extendCount': (number);
  'completionTokenHash': (Buffer);
  'grantedMs': (number);
}
