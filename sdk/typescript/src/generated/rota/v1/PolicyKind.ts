// Original file: proto/rota/v1/rota.proto

export const PolicyKind = {
  POLICY_KIND_UNSPECIFIED: 0,
  DRR: 1,
  STRICT_PRIORITY: 2,
  WFQ: 3,
  LOTTERY: 4,
  CUSTOM: 5,
  COMPLETION_AWARE: 6,
} as const;

export type PolicyKind =
  | 'POLICY_KIND_UNSPECIFIED'
  | 0
  | 'DRR'
  | 1
  | 'STRICT_PRIORITY'
  | 2
  | 'WFQ'
  | 3
  | 'LOTTERY'
  | 4
  | 'CUSTOM'
  | 5
  | 'COMPLETION_AWARE'
  | 6

export type PolicyKind__Output = typeof PolicyKind[keyof typeof PolicyKind]
