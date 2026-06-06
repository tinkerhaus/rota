// Original file: proto/rota/v1/rota.proto

export const MisfirePolicy = {
  MISFIRE_POLICY_UNSPECIFIED: 0,
  FIRE_ONCE: 1,
  FIRE_ALL: 2,
} as const;

export type MisfirePolicy =
  | 'MISFIRE_POLICY_UNSPECIFIED'
  | 0
  | 'FIRE_ONCE'
  | 1
  | 'FIRE_ALL'
  | 2

export type MisfirePolicy__Output = typeof MisfirePolicy[keyof typeof MisfirePolicy]
