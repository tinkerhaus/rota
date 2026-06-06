// Original file: proto/rota/v1/rota.proto

export const MessageState = {
  MESSAGE_STATE_UNSPECIFIED: 0,
  READY: 1,
  DELAYED: 2,
  LEASED: 3,
  DEAD: 4,
  DONE: 5,
} as const;

export type MessageState =
  | 'MESSAGE_STATE_UNSPECIFIED'
  | 0
  | 'READY'
  | 1
  | 'DELAYED'
  | 2
  | 'LEASED'
  | 3
  | 'DEAD'
  | 4
  | 'DONE'
  | 5

export type MessageState__Output = typeof MessageState[keyof typeof MessageState]
