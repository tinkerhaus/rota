// Original file: proto/rota/v1/rota.proto

export const Outcome = {
  SUCCESS: 0,
  FAILURE: 1,
} as const;

export type Outcome =
  | 'SUCCESS'
  | 0
  | 'FAILURE'
  | 1

export type Outcome__Output = typeof Outcome[keyof typeof Outcome]
