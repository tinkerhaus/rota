// Original file: proto/rota/v1/rota.proto

export const ErrorCode = {
  OK: 0,
  NOT_LEADER: 1,
  LEASE_EXPIRED: 2,
  UNKNOWN_LEASE: 3,
  POLICY_FAULT: 4,
  RATE_LIMITED: 5,
  UNKNOWN_TOKEN: 6,
  INTERNAL: 7,
} as const;

export type ErrorCode =
  | 'OK'
  | 0
  | 'NOT_LEADER'
  | 1
  | 'LEASE_EXPIRED'
  | 2
  | 'UNKNOWN_LEASE'
  | 3
  | 'POLICY_FAULT'
  | 4
  | 'RATE_LIMITED'
  | 5
  | 'UNKNOWN_TOKEN'
  | 6
  | 'INTERNAL'
  | 7

export type ErrorCode__Output = typeof ErrorCode[keyof typeof ErrorCode]
