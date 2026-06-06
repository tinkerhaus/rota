// Original file: proto/rota/v1/rota.proto


export interface SetLaneConfigRequest {
  'lane'?: (string);
  'ratePerSec'?: (number | string);
  'burst'?: (number);
}

export interface SetLaneConfigRequest__Output {
  'lane': (string);
  'ratePerSec': (number);
  'burst': (number);
}
