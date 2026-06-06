// Original file: proto/rota/v1/rota.proto

import type { LeasedMessage as _rota_v1_LeasedMessage, LeasedMessage__Output as _rota_v1_LeasedMessage__Output } from '../../rota/v1/LeasedMessage';
import type { CreditGrant as _rota_v1_CreditGrant, CreditGrant__Output as _rota_v1_CreditGrant__Output } from '../../rota/v1/CreditGrant';
import type { ControlFrame as _rota_v1_ControlFrame, ControlFrame__Output as _rota_v1_ControlFrame__Output } from '../../rota/v1/ControlFrame';
import type { StreamError as _rota_v1_StreamError, StreamError__Output as _rota_v1_StreamError__Output } from '../../rota/v1/StreamError';

export interface WorkServerMsg {
  'lease'?: (_rota_v1_LeasedMessage | null);
  'credit'?: (_rota_v1_CreditGrant | null);
  'control'?: (_rota_v1_ControlFrame | null);
  'error'?: (_rota_v1_StreamError | null);
  'msg'?: "lease"|"credit"|"control"|"error";
}

export interface WorkServerMsg__Output {
  'lease'?: (_rota_v1_LeasedMessage__Output | null);
  'credit'?: (_rota_v1_CreditGrant__Output | null);
  'control'?: (_rota_v1_ControlFrame__Output | null);
  'error'?: (_rota_v1_StreamError__Output | null);
  'msg'?: "lease"|"credit"|"control"|"error";
}
