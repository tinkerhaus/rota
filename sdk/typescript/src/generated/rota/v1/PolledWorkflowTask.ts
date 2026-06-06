// Original file: proto/rota/v1/rota.proto

import type { HistoryEvent as _rota_v1_HistoryEvent, HistoryEvent__Output as _rota_v1_HistoryEvent__Output } from '../../rota/v1/HistoryEvent';
import type { Long } from '@grpc/proto-loader';

export interface PolledWorkflowTask {
  'empty'?: (boolean);
  'runId'?: (number | string | Long);
  'leaseId'?: (number | string | Long);
  'runEpoch'?: (number);
  'historySeq'?: (number | string | Long);
  'history'?: (_rota_v1_HistoryEvent)[];
}

export interface PolledWorkflowTask__Output {
  'empty': (boolean);
  'runId': (number);
  'leaseId': (number);
  'runEpoch': (number);
  'historySeq': (number);
  'history': (_rota_v1_HistoryEvent__Output)[];
}
