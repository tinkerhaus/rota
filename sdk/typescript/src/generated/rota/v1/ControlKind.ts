// Original file: proto/rota/v1/rota.proto

export const ControlKind = {
  CONTROL_KIND_UNSPECIFIED: 0,
  PAUSE_LANE: 1,
  RESUME_LANE: 2,
  PAUSE_GROUP: 3,
  RESUME_GROUP: 4,
} as const;

export type ControlKind =
  | 'CONTROL_KIND_UNSPECIFIED'
  | 0
  | 'PAUSE_LANE'
  | 1
  | 'RESUME_LANE'
  | 2
  | 'PAUSE_GROUP'
  | 3
  | 'RESUME_GROUP'
  | 4

export type ControlKind__Output = typeof ControlKind[keyof typeof ControlKind]
