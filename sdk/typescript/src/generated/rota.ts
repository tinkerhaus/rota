import type * as grpc from '@grpc/grpc-js';
import type { EnumTypeDefinition, MessageTypeDefinition } from '@grpc/proto-loader';

import type { BrokerClient as _rota_v1_BrokerClient, BrokerDefinition as _rota_v1_BrokerDefinition } from './rota/v1/Broker';
import type { ControlClient as _rota_v1_ControlClient, ControlDefinition as _rota_v1_ControlDefinition } from './rota/v1/Control';
import type { WorkflowClient as _rota_v1_WorkflowClient, WorkflowDefinition as _rota_v1_WorkflowDefinition } from './rota/v1/Workflow';

type SubtypeConstructor<Constructor extends new (...args: any) => any, Subtype> = {
  new(...args: ConstructorParameters<Constructor>): Subtype;
};

export interface ProtoGrpcType {
  google: {
    protobuf: {
      Duration: MessageTypeDefinition
      Timestamp: MessageTypeDefinition
    }
  }
  rota: {
    v1: {
      Ack: MessageTypeDefinition
      AcquireSingletonRequest: MessageTypeDefinition
      ActivityCompletedAttrs: MessageTypeDefinition
      ActivityScheduledAttrs: MessageTypeDefinition
      ActivityTask: MessageTypeDefinition
      Broker: SubtypeConstructor<typeof grpc.Client, _rota_v1_BrokerClient> & { service: _rota_v1_BrokerDefinition }
      CancelWorkflowRequest: MessageTypeDefinition
      CancelWorkflowResponse: MessageTypeDefinition
      ClusterInfo: MessageTypeDefinition
      Complete: MessageTypeDefinition
      CompleteByTokenRequest: MessageTypeDefinition
      CompleteResult: MessageTypeDefinition
      ContinueAsNewAttrs: MessageTypeDefinition
      Control: SubtypeConstructor<typeof grpc.Client, _rota_v1_ControlClient> & { service: _rota_v1_ControlDefinition }
      ControlFrame: MessageTypeDefinition
      ControlKind: EnumTypeDefinition
      CreditGrant: MessageTypeDefinition
      CronInfo: MessageTypeDefinition
      CronOpResult: MessageTypeDefinition
      CronRef: MessageTypeDefinition
      DeadLetter: MessageTypeDefinition
      DeadLetterInfo: MessageTypeDefinition
      DescribeClusterRequest: MessageTypeDefinition
      ErrorCode: EnumTypeDefinition
      ExtendVisibility: MessageTypeDefinition
      GetStatsRequest: MessageTypeDefinition
      GetWorkflowHistoryResponse: MessageTypeDefinition
      GroupConfig: MessageTypeDefinition
      GroupFairness: MessageTypeDefinition
      GroupMeta: MessageTypeDefinition
      GroupOpResult: MessageTypeDefinition
      GroupRef: MessageTypeDefinition
      GroupStats: MessageTypeDefinition
      HealthRequest: MessageTypeDefinition
      HealthResponse: MessageTypeDefinition
      HistoryEvent: MessageTypeDefinition
      HistoryEventType: EnumTypeDefinition
      LaneConfig: MessageTypeDefinition
      LaneFairness: MessageTypeDefinition
      LaneOpResult: MessageTypeDefinition
      LaneRef: MessageTypeDefinition
      LaneStats: MessageTypeDefinition
      Lease: MessageTypeDefinition
      LeaseInfo: MessageTypeDefinition
      LeaseRequest: MessageTypeDefinition
      LeasedMessage: MessageTypeDefinition
      ListCronRequest: MessageTypeDefinition
      ListCronResponse: MessageTypeDefinition
      ListDeadLettersRequest: MessageTypeDefinition
      ListDeadLettersResponse: MessageTypeDefinition
      ListGroupsRequest: MessageTypeDefinition
      ListGroupsResponse: MessageTypeDefinition
      ListLeasesRequest: MessageTypeDefinition
      ListLeasesResponse: MessageTypeDefinition
      ListWorkflowRunsRequest: MessageTypeDefinition
      ListWorkflowRunsResponse: MessageTypeDefinition
      Message: MessageTypeDefinition
      MessagePeek: MessageTypeDefinition
      MessageSpec: MessageTypeDefinition
      MessageState: EnumTypeDefinition
      MisfirePolicy: EnumTypeDefinition
      Nack: MessageTypeDefinition
      NackMode: EnumTypeDefinition
      NextFires: MessageTypeDefinition
      NotLeader: MessageTypeDefinition
      Outcome: EnumTypeDefinition
      PauseLaneRequest: MessageTypeDefinition
      PeekMessagesRequest: MessageTypeDefinition
      PeekMessagesResponse: MessageTypeDefinition
      PeerInfo: MessageTypeDefinition
      PolicyHealth: MessageTypeDefinition
      PolicyInfo: MessageTypeDefinition
      PolicyKind: EnumTypeDefinition
      PolicyMode: EnumTypeDefinition
      PolicySource: MessageTypeDefinition
      PollTaskRequest: MessageTypeDefinition
      PolledActivityTask: MessageTypeDefinition
      PolledWorkflowTask: MessageTypeDefinition
      PublishBatchRequest: MessageTypeDefinition
      PublishBatchResponse: MessageTypeDefinition
      PublishItemResult: MessageTypeDefinition
      PublishRequest: MessageTypeDefinition
      PublishResponse: MessageTypeDefinition
      RedriveDeadLetterRequest: MessageTypeDefinition
      RedriveDeadLetterResponse: MessageTypeDefinition
      ReleaseSingletonRequest: MessageTypeDefinition
      RenewSingletonRequest: MessageTypeDefinition
      RespondActivityTaskRequest: MessageTypeDefinition
      RespondActivityTaskResponse: MessageTypeDefinition
      RespondWorkflowTaskRequest: MessageTypeDefinition
      RespondWorkflowTaskResponse: MessageTypeDefinition
      RetryBackoff: MessageTypeDefinition
      ScheduleCronRequest: MessageTypeDefinition
      SetGroupConfigRequest: MessageTypeDefinition
      SetLaneConfigRequest: MessageTypeDefinition
      SetPolicyRequest: MessageTypeDefinition
      SignalReceivedAttrs: MessageTypeDefinition
      SignalWorkflowRequest: MessageTypeDefinition
      SignalWorkflowResponse: MessageTypeDefinition
      SingletonLease: MessageTypeDefinition
      SingletonOpResult: MessageTypeDefinition
      StartWorkflowRequest: MessageTypeDefinition
      StartWorkflowResponse: MessageTypeDefinition
      StatsResponse: MessageTypeDefinition
      StreamError: MessageTypeDefinition
      TeardownRequest: MessageTypeDefinition
      TeardownResult: MessageTypeDefinition
      TimerFiredAttrs: MessageTypeDefinition
      TimerStartedAttrs: MessageTypeDefinition
      ValidatePolicyResult: MessageTypeDefinition
      WorkClientMsg: MessageTypeDefinition
      WorkServerMsg: MessageTypeDefinition
      Workflow: SubtypeConstructor<typeof grpc.Client, _rota_v1_WorkflowClient> & { service: _rota_v1_WorkflowDefinition }
      WorkflowCommandProto: MessageTypeDefinition
      WorkflowRun: MessageTypeDefinition
      WorkflowRunRef: MessageTypeDefinition
      WorkflowStatus: EnumTypeDefinition
      WorkflowTaskRef: MessageTypeDefinition
    }
  }
}

