// Original file: proto/rota/v1/rota.proto

export const WorkflowStatus = {
  WF_PENDING: 0,
  WF_RUNNING: 1,
  WF_COMPLETED: 2,
  WF_FAILED: 3,
  WF_CANCELED: 4,
  WF_CONTINUED: 5,
} as const;

export type WorkflowStatus =
  | 'WF_PENDING'
  | 0
  | 'WF_RUNNING'
  | 1
  | 'WF_COMPLETED'
  | 2
  | 'WF_FAILED'
  | 3
  | 'WF_CANCELED'
  | 4
  | 'WF_CONTINUED'
  | 5

export type WorkflowStatus__Output = typeof WorkflowStatus[keyof typeof WorkflowStatus]
