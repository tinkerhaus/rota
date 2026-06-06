// Original file: proto/rota/v1/rota.proto

import type { MessageSpec as _rota_v1_MessageSpec, MessageSpec__Output as _rota_v1_MessageSpec__Output } from '../../rota/v1/MessageSpec';

export interface PublishBatchRequest {
  'messages'?: (_rota_v1_MessageSpec)[];
  'atomic'?: (boolean);
}

export interface PublishBatchRequest__Output {
  'messages': (_rota_v1_MessageSpec__Output)[];
  'atomic': (boolean);
}
