// Original file: proto/rota/v1/rota.proto

import type { Long } from '@grpc/proto-loader';

export interface LeaseInfo {
  'lane'?: (string);
  'groupId'?: (string);
  'msgId'?: (number | string | Long);
  'leaseId'?: (number | string | Long);
  'consumerId'?: (string);
  'deadlineMs'?: (number | string | Long);
  'attempt'?: (number);
  'epoch'?: (number);
  'extendCount'?: (number);
}

export interface LeaseInfo__Output {
  'lane': (string);
  'groupId': (string);
  'msgId': (number);
  'leaseId': (number);
  'consumerId': (string);
  'deadlineMs': (number);
  'attempt': (number);
  'epoch': (number);
  'extendCount': (number);
}
