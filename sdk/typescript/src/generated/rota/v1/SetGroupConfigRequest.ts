// Original file: proto/rota/v1/rota.proto


export interface SetGroupConfigRequest {
  'lane'?: (string);
  'groupId'?: (string);
  'weight'?: (number | string);
  'batchSize'?: (number);
  '_weight'?: "weight";
  '_batchSize'?: "batchSize";
}

export interface SetGroupConfigRequest__Output {
  'lane': (string);
  'groupId': (string);
  'weight'?: (number);
  'batchSize'?: (number);
  '_weight'?: "weight";
  '_batchSize'?: "batchSize";
}
