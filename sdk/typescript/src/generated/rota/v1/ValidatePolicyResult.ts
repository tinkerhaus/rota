// Original file: proto/rota/v1/rota.proto


export interface ValidatePolicyResult {
  'ok'?: (boolean);
  'diagnostics'?: (string)[];
  'estimatedCost'?: (number | string);
}

export interface ValidatePolicyResult__Output {
  'ok': (boolean);
  'diagnostics': (string)[];
  'estimatedCost': (number);
}
