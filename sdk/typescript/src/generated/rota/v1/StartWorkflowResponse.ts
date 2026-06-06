// Original file: proto/rota/v1/rota.proto

import type { Long } from '@grpc/proto-loader';

export interface StartWorkflowResponse {
  'runId'?: (number | string | Long);
}

export interface StartWorkflowResponse__Output {
  'runId': (number);
}
