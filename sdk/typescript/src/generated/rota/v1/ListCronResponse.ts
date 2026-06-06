// Original file: proto/rota/v1/rota.proto

import type { CronInfo as _rota_v1_CronInfo, CronInfo__Output as _rota_v1_CronInfo__Output } from '../../rota/v1/CronInfo';
import type { NextFires as _rota_v1_NextFires, NextFires__Output as _rota_v1_NextFires__Output } from '../../rota/v1/NextFires';

export interface ListCronResponse {
  'crons'?: (_rota_v1_CronInfo)[];
  'nextFires'?: ({[key: string]: _rota_v1_NextFires});
}

export interface ListCronResponse__Output {
  'crons': (_rota_v1_CronInfo__Output)[];
  'nextFires': ({[key: string]: _rota_v1_NextFires__Output});
}
