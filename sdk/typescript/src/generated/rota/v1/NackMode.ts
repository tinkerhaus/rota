// Original file: proto/rota/v1/rota.proto

export const NackMode = {
  REQUEUE_NO_PENALTY: 0,
  RETRY: 1,
  DEAD_LETTER: 2,
} as const;

export type NackMode =
  | 'REQUEUE_NO_PENALTY'
  | 0
  | 'RETRY'
  | 1
  | 'DEAD_LETTER'
  | 2

export type NackMode__Output = typeof NackMode[keyof typeof NackMode]
