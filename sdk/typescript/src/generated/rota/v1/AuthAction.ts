// Original file: proto/rota/v1/rota.proto

export const AuthAction = {
  AUTH_ACTION_UNSPECIFIED: 0,
  AUTH_READ: 1,
  AUTH_PUBLISH: 2,
  AUTH_CONSUME: 3,
  AUTH_COMPLETE: 4,
  AUTH_WORKFLOW: 5,
  AUTH_CONFIGURE: 6,
  AUTH_ADMIN: 7,
} as const;

export type AuthAction =
  | 'AUTH_ACTION_UNSPECIFIED'
  | 0
  | 'AUTH_READ'
  | 1
  | 'AUTH_PUBLISH'
  | 2
  | 'AUTH_CONSUME'
  | 3
  | 'AUTH_COMPLETE'
  | 4
  | 'AUTH_WORKFLOW'
  | 5
  | 'AUTH_CONFIGURE'
  | 6
  | 'AUTH_ADMIN'
  | 7

export type AuthAction__Output = typeof AuthAction[keyof typeof AuthAction]
