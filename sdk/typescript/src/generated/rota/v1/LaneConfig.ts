// Original file: proto/rota/v1/rota.proto


export interface LaneConfig {
  'lane'?: (string);
  'ratePerSec'?: (number | string);
  'burst'?: (number);
  'paused'?: (boolean);
}

export interface LaneConfig__Output {
  'lane': (string);
  'ratePerSec': (number);
  'burst': (number);
  'paused': (boolean);
}
