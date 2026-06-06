// Original file: proto/rota/v1/rota.proto

import type { DeadLetterInfo as _rota_v1_DeadLetterInfo, DeadLetterInfo__Output as _rota_v1_DeadLetterInfo__Output } from '../../rota/v1/DeadLetterInfo';

export interface ListDeadLettersResponse {
  'deadLetters'?: (_rota_v1_DeadLetterInfo)[];
  'nextPageToken'?: (string);
}

export interface ListDeadLettersResponse__Output {
  'deadLetters': (_rota_v1_DeadLetterInfo__Output)[];
  'nextPageToken': (string);
}
