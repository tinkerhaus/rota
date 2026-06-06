// Original file: proto/rota/v1/rota.proto

export const PolicyMode = {
  POLICY_MODE_UNSPECIFIED: 0,
  SCORE: 1,
  DECIDE: 2,
} as const;

export type PolicyMode =
  | 'POLICY_MODE_UNSPECIFIED'
  | 0
  | 'SCORE'
  | 1
  | 'DECIDE'
  | 2

export type PolicyMode__Output = typeof PolicyMode[keyof typeof PolicyMode]
