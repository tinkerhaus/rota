// Original file: proto/rota/v1/rota.proto


export interface HealthResponse {
  'serving'?: (boolean);
  'hasQuorum'?: (boolean);
  'isLeader'?: (boolean);
}

export interface HealthResponse__Output {
  'serving': (boolean);
  'hasQuorum': (boolean);
  'isLeader': (boolean);
}
