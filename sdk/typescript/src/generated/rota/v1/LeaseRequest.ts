// Original file: proto/rota/v1/rota.proto


export interface LeaseRequest {
  'lane'?: (string);
  'credit'?: (number);
  'groupAllow'?: (string)[];
  'groupDeny'?: (string)[];
  'consumerId'?: (string);
}

export interface LeaseRequest__Output {
  'lane': (string);
  'credit': (number);
  'groupAllow': (string)[];
  'groupDeny': (string)[];
  'consumerId': (string);
}
