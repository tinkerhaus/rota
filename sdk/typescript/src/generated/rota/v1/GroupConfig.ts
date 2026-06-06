// Original file: proto/rota/v1/rota.proto


export interface GroupConfig {
  'lane'?: (string);
  'groupId'?: (string);
  'weight'?: (number | string);
  'batchSize'?: (number);
  'paused'?: (boolean);
}

export interface GroupConfig__Output {
  'lane': (string);
  'groupId': (string);
  'weight': (number);
  'batchSize': (number);
  'paused': (boolean);
}
