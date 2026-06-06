// Original file: proto/rota/v1/rota.proto


export interface StartWorkflowRequest {
  'workflowType'?: (string);
  'tenantId'?: (string);
  'input'?: (Buffer | Uint8Array | string);
}

export interface StartWorkflowRequest__Output {
  'workflowType': (string);
  'tenantId': (string);
  'input': (Buffer);
}
