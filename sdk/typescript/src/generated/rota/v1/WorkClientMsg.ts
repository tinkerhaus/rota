// Original file: proto/rota/v1/rota.proto

import type { LeaseRequest as _rota_v1_LeaseRequest, LeaseRequest__Output as _rota_v1_LeaseRequest__Output } from '../../rota/v1/LeaseRequest';
import type { Ack as _rota_v1_Ack, Ack__Output as _rota_v1_Ack__Output } from '../../rota/v1/Ack';
import type { Nack as _rota_v1_Nack, Nack__Output as _rota_v1_Nack__Output } from '../../rota/v1/Nack';
import type { ExtendVisibility as _rota_v1_ExtendVisibility, ExtendVisibility__Output as _rota_v1_ExtendVisibility__Output } from '../../rota/v1/ExtendVisibility';
import type { Complete as _rota_v1_Complete, Complete__Output as _rota_v1_Complete__Output } from '../../rota/v1/Complete';

export interface WorkClientMsg {
  'leaseRequest'?: (_rota_v1_LeaseRequest | null);
  'ack'?: (_rota_v1_Ack | null);
  'nack'?: (_rota_v1_Nack | null);
  'extend'?: (_rota_v1_ExtendVisibility | null);
  'complete'?: (_rota_v1_Complete | null);
  'msg'?: "leaseRequest"|"ack"|"nack"|"extend"|"complete";
}

export interface WorkClientMsg__Output {
  'leaseRequest'?: (_rota_v1_LeaseRequest__Output | null);
  'ack'?: (_rota_v1_Ack__Output | null);
  'nack'?: (_rota_v1_Nack__Output | null);
  'extend'?: (_rota_v1_ExtendVisibility__Output | null);
  'complete'?: (_rota_v1_Complete__Output | null);
  'msg'?: "leaseRequest"|"ack"|"nack"|"extend"|"complete";
}
