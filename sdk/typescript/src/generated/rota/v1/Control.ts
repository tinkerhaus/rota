// Original file: proto/rota/v1/rota.proto

import type * as grpc from '@grpc/grpc-js'
import type { MethodDefinition } from '@grpc/proto-loader'
import type { AcquireSingletonRequest as _rota_v1_AcquireSingletonRequest, AcquireSingletonRequest__Output as _rota_v1_AcquireSingletonRequest__Output } from '../../rota/v1/AcquireSingletonRequest';
import type { ClusterInfo as _rota_v1_ClusterInfo, ClusterInfo__Output as _rota_v1_ClusterInfo__Output } from '../../rota/v1/ClusterInfo';
import type { CompleteByTokenRequest as _rota_v1_CompleteByTokenRequest, CompleteByTokenRequest__Output as _rota_v1_CompleteByTokenRequest__Output } from '../../rota/v1/CompleteByTokenRequest';
import type { CompleteResult as _rota_v1_CompleteResult, CompleteResult__Output as _rota_v1_CompleteResult__Output } from '../../rota/v1/CompleteResult';
import type { CronInfo as _rota_v1_CronInfo, CronInfo__Output as _rota_v1_CronInfo__Output } from '../../rota/v1/CronInfo';
import type { CronOpResult as _rota_v1_CronOpResult, CronOpResult__Output as _rota_v1_CronOpResult__Output } from '../../rota/v1/CronOpResult';
import type { CronRef as _rota_v1_CronRef, CronRef__Output as _rota_v1_CronRef__Output } from '../../rota/v1/CronRef';
import type { DescribeClusterRequest as _rota_v1_DescribeClusterRequest, DescribeClusterRequest__Output as _rota_v1_DescribeClusterRequest__Output } from '../../rota/v1/DescribeClusterRequest';
import type { GetStatsRequest as _rota_v1_GetStatsRequest, GetStatsRequest__Output as _rota_v1_GetStatsRequest__Output } from '../../rota/v1/GetStatsRequest';
import type { GroupConfig as _rota_v1_GroupConfig, GroupConfig__Output as _rota_v1_GroupConfig__Output } from '../../rota/v1/GroupConfig';
import type { GroupOpResult as _rota_v1_GroupOpResult, GroupOpResult__Output as _rota_v1_GroupOpResult__Output } from '../../rota/v1/GroupOpResult';
import type { GroupRef as _rota_v1_GroupRef, GroupRef__Output as _rota_v1_GroupRef__Output } from '../../rota/v1/GroupRef';
import type { HealthRequest as _rota_v1_HealthRequest, HealthRequest__Output as _rota_v1_HealthRequest__Output } from '../../rota/v1/HealthRequest';
import type { HealthResponse as _rota_v1_HealthResponse, HealthResponse__Output as _rota_v1_HealthResponse__Output } from '../../rota/v1/HealthResponse';
import type { LaneConfig as _rota_v1_LaneConfig, LaneConfig__Output as _rota_v1_LaneConfig__Output } from '../../rota/v1/LaneConfig';
import type { LaneFairness as _rota_v1_LaneFairness, LaneFairness__Output as _rota_v1_LaneFairness__Output } from '../../rota/v1/LaneFairness';
import type { LaneOpResult as _rota_v1_LaneOpResult, LaneOpResult__Output as _rota_v1_LaneOpResult__Output } from '../../rota/v1/LaneOpResult';
import type { LaneRef as _rota_v1_LaneRef, LaneRef__Output as _rota_v1_LaneRef__Output } from '../../rota/v1/LaneRef';
import type { ListCronRequest as _rota_v1_ListCronRequest, ListCronRequest__Output as _rota_v1_ListCronRequest__Output } from '../../rota/v1/ListCronRequest';
import type { ListCronResponse as _rota_v1_ListCronResponse, ListCronResponse__Output as _rota_v1_ListCronResponse__Output } from '../../rota/v1/ListCronResponse';
import type { ListDeadLettersRequest as _rota_v1_ListDeadLettersRequest, ListDeadLettersRequest__Output as _rota_v1_ListDeadLettersRequest__Output } from '../../rota/v1/ListDeadLettersRequest';
import type { ListDeadLettersResponse as _rota_v1_ListDeadLettersResponse, ListDeadLettersResponse__Output as _rota_v1_ListDeadLettersResponse__Output } from '../../rota/v1/ListDeadLettersResponse';
import type { ListGroupsRequest as _rota_v1_ListGroupsRequest, ListGroupsRequest__Output as _rota_v1_ListGroupsRequest__Output } from '../../rota/v1/ListGroupsRequest';
import type { ListGroupsResponse as _rota_v1_ListGroupsResponse, ListGroupsResponse__Output as _rota_v1_ListGroupsResponse__Output } from '../../rota/v1/ListGroupsResponse';
import type { ListLeasesRequest as _rota_v1_ListLeasesRequest, ListLeasesRequest__Output as _rota_v1_ListLeasesRequest__Output } from '../../rota/v1/ListLeasesRequest';
import type { ListLeasesResponse as _rota_v1_ListLeasesResponse, ListLeasesResponse__Output as _rota_v1_ListLeasesResponse__Output } from '../../rota/v1/ListLeasesResponse';
import type { PauseLaneRequest as _rota_v1_PauseLaneRequest, PauseLaneRequest__Output as _rota_v1_PauseLaneRequest__Output } from '../../rota/v1/PauseLaneRequest';
import type { PeekMessagesRequest as _rota_v1_PeekMessagesRequest, PeekMessagesRequest__Output as _rota_v1_PeekMessagesRequest__Output } from '../../rota/v1/PeekMessagesRequest';
import type { PeekMessagesResponse as _rota_v1_PeekMessagesResponse, PeekMessagesResponse__Output as _rota_v1_PeekMessagesResponse__Output } from '../../rota/v1/PeekMessagesResponse';
import type { PolicyHealth as _rota_v1_PolicyHealth, PolicyHealth__Output as _rota_v1_PolicyHealth__Output } from '../../rota/v1/PolicyHealth';
import type { PolicyInfo as _rota_v1_PolicyInfo, PolicyInfo__Output as _rota_v1_PolicyInfo__Output } from '../../rota/v1/PolicyInfo';
import type { RedriveDeadLetterRequest as _rota_v1_RedriveDeadLetterRequest, RedriveDeadLetterRequest__Output as _rota_v1_RedriveDeadLetterRequest__Output } from '../../rota/v1/RedriveDeadLetterRequest';
import type { RedriveDeadLetterResponse as _rota_v1_RedriveDeadLetterResponse, RedriveDeadLetterResponse__Output as _rota_v1_RedriveDeadLetterResponse__Output } from '../../rota/v1/RedriveDeadLetterResponse';
import type { ReleaseSingletonRequest as _rota_v1_ReleaseSingletonRequest, ReleaseSingletonRequest__Output as _rota_v1_ReleaseSingletonRequest__Output } from '../../rota/v1/ReleaseSingletonRequest';
import type { RenewSingletonRequest as _rota_v1_RenewSingletonRequest, RenewSingletonRequest__Output as _rota_v1_RenewSingletonRequest__Output } from '../../rota/v1/RenewSingletonRequest';
import type { ScheduleCronRequest as _rota_v1_ScheduleCronRequest, ScheduleCronRequest__Output as _rota_v1_ScheduleCronRequest__Output } from '../../rota/v1/ScheduleCronRequest';
import type { SetGroupConfigRequest as _rota_v1_SetGroupConfigRequest, SetGroupConfigRequest__Output as _rota_v1_SetGroupConfigRequest__Output } from '../../rota/v1/SetGroupConfigRequest';
import type { SetLaneConfigRequest as _rota_v1_SetLaneConfigRequest, SetLaneConfigRequest__Output as _rota_v1_SetLaneConfigRequest__Output } from '../../rota/v1/SetLaneConfigRequest';
import type { SetPolicyRequest as _rota_v1_SetPolicyRequest, SetPolicyRequest__Output as _rota_v1_SetPolicyRequest__Output } from '../../rota/v1/SetPolicyRequest';
import type { SingletonLease as _rota_v1_SingletonLease, SingletonLease__Output as _rota_v1_SingletonLease__Output } from '../../rota/v1/SingletonLease';
import type { SingletonOpResult as _rota_v1_SingletonOpResult, SingletonOpResult__Output as _rota_v1_SingletonOpResult__Output } from '../../rota/v1/SingletonOpResult';
import type { StatsResponse as _rota_v1_StatsResponse, StatsResponse__Output as _rota_v1_StatsResponse__Output } from '../../rota/v1/StatsResponse';
import type { TeardownRequest as _rota_v1_TeardownRequest, TeardownRequest__Output as _rota_v1_TeardownRequest__Output } from '../../rota/v1/TeardownRequest';
import type { TeardownResult as _rota_v1_TeardownResult, TeardownResult__Output as _rota_v1_TeardownResult__Output } from '../../rota/v1/TeardownResult';
import type { ValidatePolicyResult as _rota_v1_ValidatePolicyResult, ValidatePolicyResult__Output as _rota_v1_ValidatePolicyResult__Output } from '../../rota/v1/ValidatePolicyResult';

export interface ControlClient extends grpc.Client {
  AcquireSingletonLease(argument: _rota_v1_AcquireSingletonRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_SingletonLease__Output>): grpc.ClientUnaryCall;
  AcquireSingletonLease(argument: _rota_v1_AcquireSingletonRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_SingletonLease__Output>): grpc.ClientUnaryCall;
  AcquireSingletonLease(argument: _rota_v1_AcquireSingletonRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_SingletonLease__Output>): grpc.ClientUnaryCall;
  AcquireSingletonLease(argument: _rota_v1_AcquireSingletonRequest, callback: grpc.requestCallback<_rota_v1_SingletonLease__Output>): grpc.ClientUnaryCall;
  acquireSingletonLease(argument: _rota_v1_AcquireSingletonRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_SingletonLease__Output>): grpc.ClientUnaryCall;
  acquireSingletonLease(argument: _rota_v1_AcquireSingletonRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_SingletonLease__Output>): grpc.ClientUnaryCall;
  acquireSingletonLease(argument: _rota_v1_AcquireSingletonRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_SingletonLease__Output>): grpc.ClientUnaryCall;
  acquireSingletonLease(argument: _rota_v1_AcquireSingletonRequest, callback: grpc.requestCallback<_rota_v1_SingletonLease__Output>): grpc.ClientUnaryCall;
  
  CancelGroup(argument: _rota_v1_GroupRef, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_GroupOpResult__Output>): grpc.ClientUnaryCall;
  CancelGroup(argument: _rota_v1_GroupRef, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_GroupOpResult__Output>): grpc.ClientUnaryCall;
  CancelGroup(argument: _rota_v1_GroupRef, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_GroupOpResult__Output>): grpc.ClientUnaryCall;
  CancelGroup(argument: _rota_v1_GroupRef, callback: grpc.requestCallback<_rota_v1_GroupOpResult__Output>): grpc.ClientUnaryCall;
  cancelGroup(argument: _rota_v1_GroupRef, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_GroupOpResult__Output>): grpc.ClientUnaryCall;
  cancelGroup(argument: _rota_v1_GroupRef, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_GroupOpResult__Output>): grpc.ClientUnaryCall;
  cancelGroup(argument: _rota_v1_GroupRef, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_GroupOpResult__Output>): grpc.ClientUnaryCall;
  cancelGroup(argument: _rota_v1_GroupRef, callback: grpc.requestCallback<_rota_v1_GroupOpResult__Output>): grpc.ClientUnaryCall;
  
  CompleteByToken(argument: _rota_v1_CompleteByTokenRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_CompleteResult__Output>): grpc.ClientUnaryCall;
  CompleteByToken(argument: _rota_v1_CompleteByTokenRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_CompleteResult__Output>): grpc.ClientUnaryCall;
  CompleteByToken(argument: _rota_v1_CompleteByTokenRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_CompleteResult__Output>): grpc.ClientUnaryCall;
  CompleteByToken(argument: _rota_v1_CompleteByTokenRequest, callback: grpc.requestCallback<_rota_v1_CompleteResult__Output>): grpc.ClientUnaryCall;
  completeByToken(argument: _rota_v1_CompleteByTokenRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_CompleteResult__Output>): grpc.ClientUnaryCall;
  completeByToken(argument: _rota_v1_CompleteByTokenRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_CompleteResult__Output>): grpc.ClientUnaryCall;
  completeByToken(argument: _rota_v1_CompleteByTokenRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_CompleteResult__Output>): grpc.ClientUnaryCall;
  completeByToken(argument: _rota_v1_CompleteByTokenRequest, callback: grpc.requestCallback<_rota_v1_CompleteResult__Output>): grpc.ClientUnaryCall;
  
  DeleteCron(argument: _rota_v1_CronRef, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_CronOpResult__Output>): grpc.ClientUnaryCall;
  DeleteCron(argument: _rota_v1_CronRef, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_CronOpResult__Output>): grpc.ClientUnaryCall;
  DeleteCron(argument: _rota_v1_CronRef, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_CronOpResult__Output>): grpc.ClientUnaryCall;
  DeleteCron(argument: _rota_v1_CronRef, callback: grpc.requestCallback<_rota_v1_CronOpResult__Output>): grpc.ClientUnaryCall;
  deleteCron(argument: _rota_v1_CronRef, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_CronOpResult__Output>): grpc.ClientUnaryCall;
  deleteCron(argument: _rota_v1_CronRef, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_CronOpResult__Output>): grpc.ClientUnaryCall;
  deleteCron(argument: _rota_v1_CronRef, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_CronOpResult__Output>): grpc.ClientUnaryCall;
  deleteCron(argument: _rota_v1_CronRef, callback: grpc.requestCallback<_rota_v1_CronOpResult__Output>): grpc.ClientUnaryCall;
  
  DescribeCluster(argument: _rota_v1_DescribeClusterRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_ClusterInfo__Output>): grpc.ClientUnaryCall;
  DescribeCluster(argument: _rota_v1_DescribeClusterRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_ClusterInfo__Output>): grpc.ClientUnaryCall;
  DescribeCluster(argument: _rota_v1_DescribeClusterRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_ClusterInfo__Output>): grpc.ClientUnaryCall;
  DescribeCluster(argument: _rota_v1_DescribeClusterRequest, callback: grpc.requestCallback<_rota_v1_ClusterInfo__Output>): grpc.ClientUnaryCall;
  describeCluster(argument: _rota_v1_DescribeClusterRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_ClusterInfo__Output>): grpc.ClientUnaryCall;
  describeCluster(argument: _rota_v1_DescribeClusterRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_ClusterInfo__Output>): grpc.ClientUnaryCall;
  describeCluster(argument: _rota_v1_DescribeClusterRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_ClusterInfo__Output>): grpc.ClientUnaryCall;
  describeCluster(argument: _rota_v1_DescribeClusterRequest, callback: grpc.requestCallback<_rota_v1_ClusterInfo__Output>): grpc.ClientUnaryCall;
  
  GetGroupConfig(argument: _rota_v1_GroupRef, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_GroupConfig__Output>): grpc.ClientUnaryCall;
  GetGroupConfig(argument: _rota_v1_GroupRef, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_GroupConfig__Output>): grpc.ClientUnaryCall;
  GetGroupConfig(argument: _rota_v1_GroupRef, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_GroupConfig__Output>): grpc.ClientUnaryCall;
  GetGroupConfig(argument: _rota_v1_GroupRef, callback: grpc.requestCallback<_rota_v1_GroupConfig__Output>): grpc.ClientUnaryCall;
  getGroupConfig(argument: _rota_v1_GroupRef, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_GroupConfig__Output>): grpc.ClientUnaryCall;
  getGroupConfig(argument: _rota_v1_GroupRef, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_GroupConfig__Output>): grpc.ClientUnaryCall;
  getGroupConfig(argument: _rota_v1_GroupRef, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_GroupConfig__Output>): grpc.ClientUnaryCall;
  getGroupConfig(argument: _rota_v1_GroupRef, callback: grpc.requestCallback<_rota_v1_GroupConfig__Output>): grpc.ClientUnaryCall;
  
  GetLaneFairness(argument: _rota_v1_LaneRef, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_LaneFairness__Output>): grpc.ClientUnaryCall;
  GetLaneFairness(argument: _rota_v1_LaneRef, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_LaneFairness__Output>): grpc.ClientUnaryCall;
  GetLaneFairness(argument: _rota_v1_LaneRef, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_LaneFairness__Output>): grpc.ClientUnaryCall;
  GetLaneFairness(argument: _rota_v1_LaneRef, callback: grpc.requestCallback<_rota_v1_LaneFairness__Output>): grpc.ClientUnaryCall;
  getLaneFairness(argument: _rota_v1_LaneRef, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_LaneFairness__Output>): grpc.ClientUnaryCall;
  getLaneFairness(argument: _rota_v1_LaneRef, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_LaneFairness__Output>): grpc.ClientUnaryCall;
  getLaneFairness(argument: _rota_v1_LaneRef, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_LaneFairness__Output>): grpc.ClientUnaryCall;
  getLaneFairness(argument: _rota_v1_LaneRef, callback: grpc.requestCallback<_rota_v1_LaneFairness__Output>): grpc.ClientUnaryCall;
  
  GetPolicy(argument: _rota_v1_LaneRef, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_PolicyInfo__Output>): grpc.ClientUnaryCall;
  GetPolicy(argument: _rota_v1_LaneRef, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_PolicyInfo__Output>): grpc.ClientUnaryCall;
  GetPolicy(argument: _rota_v1_LaneRef, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_PolicyInfo__Output>): grpc.ClientUnaryCall;
  GetPolicy(argument: _rota_v1_LaneRef, callback: grpc.requestCallback<_rota_v1_PolicyInfo__Output>): grpc.ClientUnaryCall;
  getPolicy(argument: _rota_v1_LaneRef, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_PolicyInfo__Output>): grpc.ClientUnaryCall;
  getPolicy(argument: _rota_v1_LaneRef, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_PolicyInfo__Output>): grpc.ClientUnaryCall;
  getPolicy(argument: _rota_v1_LaneRef, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_PolicyInfo__Output>): grpc.ClientUnaryCall;
  getPolicy(argument: _rota_v1_LaneRef, callback: grpc.requestCallback<_rota_v1_PolicyInfo__Output>): grpc.ClientUnaryCall;
  
  GetPolicyHealth(argument: _rota_v1_LaneRef, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_PolicyHealth__Output>): grpc.ClientUnaryCall;
  GetPolicyHealth(argument: _rota_v1_LaneRef, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_PolicyHealth__Output>): grpc.ClientUnaryCall;
  GetPolicyHealth(argument: _rota_v1_LaneRef, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_PolicyHealth__Output>): grpc.ClientUnaryCall;
  GetPolicyHealth(argument: _rota_v1_LaneRef, callback: grpc.requestCallback<_rota_v1_PolicyHealth__Output>): grpc.ClientUnaryCall;
  getPolicyHealth(argument: _rota_v1_LaneRef, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_PolicyHealth__Output>): grpc.ClientUnaryCall;
  getPolicyHealth(argument: _rota_v1_LaneRef, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_PolicyHealth__Output>): grpc.ClientUnaryCall;
  getPolicyHealth(argument: _rota_v1_LaneRef, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_PolicyHealth__Output>): grpc.ClientUnaryCall;
  getPolicyHealth(argument: _rota_v1_LaneRef, callback: grpc.requestCallback<_rota_v1_PolicyHealth__Output>): grpc.ClientUnaryCall;
  
  GetStats(argument: _rota_v1_GetStatsRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_StatsResponse__Output>): grpc.ClientUnaryCall;
  GetStats(argument: _rota_v1_GetStatsRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_StatsResponse__Output>): grpc.ClientUnaryCall;
  GetStats(argument: _rota_v1_GetStatsRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_StatsResponse__Output>): grpc.ClientUnaryCall;
  GetStats(argument: _rota_v1_GetStatsRequest, callback: grpc.requestCallback<_rota_v1_StatsResponse__Output>): grpc.ClientUnaryCall;
  getStats(argument: _rota_v1_GetStatsRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_StatsResponse__Output>): grpc.ClientUnaryCall;
  getStats(argument: _rota_v1_GetStatsRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_StatsResponse__Output>): grpc.ClientUnaryCall;
  getStats(argument: _rota_v1_GetStatsRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_StatsResponse__Output>): grpc.ClientUnaryCall;
  getStats(argument: _rota_v1_GetStatsRequest, callback: grpc.requestCallback<_rota_v1_StatsResponse__Output>): grpc.ClientUnaryCall;
  
  Health(argument: _rota_v1_HealthRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_HealthResponse__Output>): grpc.ClientUnaryCall;
  Health(argument: _rota_v1_HealthRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_HealthResponse__Output>): grpc.ClientUnaryCall;
  Health(argument: _rota_v1_HealthRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_HealthResponse__Output>): grpc.ClientUnaryCall;
  Health(argument: _rota_v1_HealthRequest, callback: grpc.requestCallback<_rota_v1_HealthResponse__Output>): grpc.ClientUnaryCall;
  health(argument: _rota_v1_HealthRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_HealthResponse__Output>): grpc.ClientUnaryCall;
  health(argument: _rota_v1_HealthRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_HealthResponse__Output>): grpc.ClientUnaryCall;
  health(argument: _rota_v1_HealthRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_HealthResponse__Output>): grpc.ClientUnaryCall;
  health(argument: _rota_v1_HealthRequest, callback: grpc.requestCallback<_rota_v1_HealthResponse__Output>): grpc.ClientUnaryCall;
  
  ListCron(argument: _rota_v1_ListCronRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_ListCronResponse__Output>): grpc.ClientUnaryCall;
  ListCron(argument: _rota_v1_ListCronRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_ListCronResponse__Output>): grpc.ClientUnaryCall;
  ListCron(argument: _rota_v1_ListCronRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_ListCronResponse__Output>): grpc.ClientUnaryCall;
  ListCron(argument: _rota_v1_ListCronRequest, callback: grpc.requestCallback<_rota_v1_ListCronResponse__Output>): grpc.ClientUnaryCall;
  listCron(argument: _rota_v1_ListCronRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_ListCronResponse__Output>): grpc.ClientUnaryCall;
  listCron(argument: _rota_v1_ListCronRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_ListCronResponse__Output>): grpc.ClientUnaryCall;
  listCron(argument: _rota_v1_ListCronRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_ListCronResponse__Output>): grpc.ClientUnaryCall;
  listCron(argument: _rota_v1_ListCronRequest, callback: grpc.requestCallback<_rota_v1_ListCronResponse__Output>): grpc.ClientUnaryCall;
  
  ListDeadLetters(argument: _rota_v1_ListDeadLettersRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_ListDeadLettersResponse__Output>): grpc.ClientUnaryCall;
  ListDeadLetters(argument: _rota_v1_ListDeadLettersRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_ListDeadLettersResponse__Output>): grpc.ClientUnaryCall;
  ListDeadLetters(argument: _rota_v1_ListDeadLettersRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_ListDeadLettersResponse__Output>): grpc.ClientUnaryCall;
  ListDeadLetters(argument: _rota_v1_ListDeadLettersRequest, callback: grpc.requestCallback<_rota_v1_ListDeadLettersResponse__Output>): grpc.ClientUnaryCall;
  listDeadLetters(argument: _rota_v1_ListDeadLettersRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_ListDeadLettersResponse__Output>): grpc.ClientUnaryCall;
  listDeadLetters(argument: _rota_v1_ListDeadLettersRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_ListDeadLettersResponse__Output>): grpc.ClientUnaryCall;
  listDeadLetters(argument: _rota_v1_ListDeadLettersRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_ListDeadLettersResponse__Output>): grpc.ClientUnaryCall;
  listDeadLetters(argument: _rota_v1_ListDeadLettersRequest, callback: grpc.requestCallback<_rota_v1_ListDeadLettersResponse__Output>): grpc.ClientUnaryCall;
  
  ListGroups(argument: _rota_v1_ListGroupsRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_ListGroupsResponse__Output>): grpc.ClientUnaryCall;
  ListGroups(argument: _rota_v1_ListGroupsRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_ListGroupsResponse__Output>): grpc.ClientUnaryCall;
  ListGroups(argument: _rota_v1_ListGroupsRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_ListGroupsResponse__Output>): grpc.ClientUnaryCall;
  ListGroups(argument: _rota_v1_ListGroupsRequest, callback: grpc.requestCallback<_rota_v1_ListGroupsResponse__Output>): grpc.ClientUnaryCall;
  listGroups(argument: _rota_v1_ListGroupsRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_ListGroupsResponse__Output>): grpc.ClientUnaryCall;
  listGroups(argument: _rota_v1_ListGroupsRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_ListGroupsResponse__Output>): grpc.ClientUnaryCall;
  listGroups(argument: _rota_v1_ListGroupsRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_ListGroupsResponse__Output>): grpc.ClientUnaryCall;
  listGroups(argument: _rota_v1_ListGroupsRequest, callback: grpc.requestCallback<_rota_v1_ListGroupsResponse__Output>): grpc.ClientUnaryCall;
  
  ListLeases(argument: _rota_v1_ListLeasesRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_ListLeasesResponse__Output>): grpc.ClientUnaryCall;
  ListLeases(argument: _rota_v1_ListLeasesRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_ListLeasesResponse__Output>): grpc.ClientUnaryCall;
  ListLeases(argument: _rota_v1_ListLeasesRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_ListLeasesResponse__Output>): grpc.ClientUnaryCall;
  ListLeases(argument: _rota_v1_ListLeasesRequest, callback: grpc.requestCallback<_rota_v1_ListLeasesResponse__Output>): grpc.ClientUnaryCall;
  listLeases(argument: _rota_v1_ListLeasesRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_ListLeasesResponse__Output>): grpc.ClientUnaryCall;
  listLeases(argument: _rota_v1_ListLeasesRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_ListLeasesResponse__Output>): grpc.ClientUnaryCall;
  listLeases(argument: _rota_v1_ListLeasesRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_ListLeasesResponse__Output>): grpc.ClientUnaryCall;
  listLeases(argument: _rota_v1_ListLeasesRequest, callback: grpc.requestCallback<_rota_v1_ListLeasesResponse__Output>): grpc.ClientUnaryCall;
  
  PauseCron(argument: _rota_v1_CronRef, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_CronInfo__Output>): grpc.ClientUnaryCall;
  PauseCron(argument: _rota_v1_CronRef, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_CronInfo__Output>): grpc.ClientUnaryCall;
  PauseCron(argument: _rota_v1_CronRef, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_CronInfo__Output>): grpc.ClientUnaryCall;
  PauseCron(argument: _rota_v1_CronRef, callback: grpc.requestCallback<_rota_v1_CronInfo__Output>): grpc.ClientUnaryCall;
  pauseCron(argument: _rota_v1_CronRef, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_CronInfo__Output>): grpc.ClientUnaryCall;
  pauseCron(argument: _rota_v1_CronRef, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_CronInfo__Output>): grpc.ClientUnaryCall;
  pauseCron(argument: _rota_v1_CronRef, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_CronInfo__Output>): grpc.ClientUnaryCall;
  pauseCron(argument: _rota_v1_CronRef, callback: grpc.requestCallback<_rota_v1_CronInfo__Output>): grpc.ClientUnaryCall;
  
  PauseGroup(argument: _rota_v1_GroupRef, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_GroupConfig__Output>): grpc.ClientUnaryCall;
  PauseGroup(argument: _rota_v1_GroupRef, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_GroupConfig__Output>): grpc.ClientUnaryCall;
  PauseGroup(argument: _rota_v1_GroupRef, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_GroupConfig__Output>): grpc.ClientUnaryCall;
  PauseGroup(argument: _rota_v1_GroupRef, callback: grpc.requestCallback<_rota_v1_GroupConfig__Output>): grpc.ClientUnaryCall;
  pauseGroup(argument: _rota_v1_GroupRef, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_GroupConfig__Output>): grpc.ClientUnaryCall;
  pauseGroup(argument: _rota_v1_GroupRef, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_GroupConfig__Output>): grpc.ClientUnaryCall;
  pauseGroup(argument: _rota_v1_GroupRef, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_GroupConfig__Output>): grpc.ClientUnaryCall;
  pauseGroup(argument: _rota_v1_GroupRef, callback: grpc.requestCallback<_rota_v1_GroupConfig__Output>): grpc.ClientUnaryCall;
  
  PauseLane(argument: _rota_v1_PauseLaneRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_LaneOpResult__Output>): grpc.ClientUnaryCall;
  PauseLane(argument: _rota_v1_PauseLaneRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_LaneOpResult__Output>): grpc.ClientUnaryCall;
  PauseLane(argument: _rota_v1_PauseLaneRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_LaneOpResult__Output>): grpc.ClientUnaryCall;
  PauseLane(argument: _rota_v1_PauseLaneRequest, callback: grpc.requestCallback<_rota_v1_LaneOpResult__Output>): grpc.ClientUnaryCall;
  pauseLane(argument: _rota_v1_PauseLaneRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_LaneOpResult__Output>): grpc.ClientUnaryCall;
  pauseLane(argument: _rota_v1_PauseLaneRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_LaneOpResult__Output>): grpc.ClientUnaryCall;
  pauseLane(argument: _rota_v1_PauseLaneRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_LaneOpResult__Output>): grpc.ClientUnaryCall;
  pauseLane(argument: _rota_v1_PauseLaneRequest, callback: grpc.requestCallback<_rota_v1_LaneOpResult__Output>): grpc.ClientUnaryCall;
  
  PeekMessages(argument: _rota_v1_PeekMessagesRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_PeekMessagesResponse__Output>): grpc.ClientUnaryCall;
  PeekMessages(argument: _rota_v1_PeekMessagesRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_PeekMessagesResponse__Output>): grpc.ClientUnaryCall;
  PeekMessages(argument: _rota_v1_PeekMessagesRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_PeekMessagesResponse__Output>): grpc.ClientUnaryCall;
  PeekMessages(argument: _rota_v1_PeekMessagesRequest, callback: grpc.requestCallback<_rota_v1_PeekMessagesResponse__Output>): grpc.ClientUnaryCall;
  peekMessages(argument: _rota_v1_PeekMessagesRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_PeekMessagesResponse__Output>): grpc.ClientUnaryCall;
  peekMessages(argument: _rota_v1_PeekMessagesRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_PeekMessagesResponse__Output>): grpc.ClientUnaryCall;
  peekMessages(argument: _rota_v1_PeekMessagesRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_PeekMessagesResponse__Output>): grpc.ClientUnaryCall;
  peekMessages(argument: _rota_v1_PeekMessagesRequest, callback: grpc.requestCallback<_rota_v1_PeekMessagesResponse__Output>): grpc.ClientUnaryCall;
  
  PurgeGroup(argument: _rota_v1_GroupRef, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_GroupOpResult__Output>): grpc.ClientUnaryCall;
  PurgeGroup(argument: _rota_v1_GroupRef, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_GroupOpResult__Output>): grpc.ClientUnaryCall;
  PurgeGroup(argument: _rota_v1_GroupRef, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_GroupOpResult__Output>): grpc.ClientUnaryCall;
  PurgeGroup(argument: _rota_v1_GroupRef, callback: grpc.requestCallback<_rota_v1_GroupOpResult__Output>): grpc.ClientUnaryCall;
  purgeGroup(argument: _rota_v1_GroupRef, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_GroupOpResult__Output>): grpc.ClientUnaryCall;
  purgeGroup(argument: _rota_v1_GroupRef, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_GroupOpResult__Output>): grpc.ClientUnaryCall;
  purgeGroup(argument: _rota_v1_GroupRef, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_GroupOpResult__Output>): grpc.ClientUnaryCall;
  purgeGroup(argument: _rota_v1_GroupRef, callback: grpc.requestCallback<_rota_v1_GroupOpResult__Output>): grpc.ClientUnaryCall;
  
  ReapGroup(argument: _rota_v1_GroupRef, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_GroupOpResult__Output>): grpc.ClientUnaryCall;
  ReapGroup(argument: _rota_v1_GroupRef, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_GroupOpResult__Output>): grpc.ClientUnaryCall;
  ReapGroup(argument: _rota_v1_GroupRef, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_GroupOpResult__Output>): grpc.ClientUnaryCall;
  ReapGroup(argument: _rota_v1_GroupRef, callback: grpc.requestCallback<_rota_v1_GroupOpResult__Output>): grpc.ClientUnaryCall;
  reapGroup(argument: _rota_v1_GroupRef, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_GroupOpResult__Output>): grpc.ClientUnaryCall;
  reapGroup(argument: _rota_v1_GroupRef, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_GroupOpResult__Output>): grpc.ClientUnaryCall;
  reapGroup(argument: _rota_v1_GroupRef, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_GroupOpResult__Output>): grpc.ClientUnaryCall;
  reapGroup(argument: _rota_v1_GroupRef, callback: grpc.requestCallback<_rota_v1_GroupOpResult__Output>): grpc.ClientUnaryCall;
  
  RedriveDeadLetter(argument: _rota_v1_RedriveDeadLetterRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_RedriveDeadLetterResponse__Output>): grpc.ClientUnaryCall;
  RedriveDeadLetter(argument: _rota_v1_RedriveDeadLetterRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_RedriveDeadLetterResponse__Output>): grpc.ClientUnaryCall;
  RedriveDeadLetter(argument: _rota_v1_RedriveDeadLetterRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_RedriveDeadLetterResponse__Output>): grpc.ClientUnaryCall;
  RedriveDeadLetter(argument: _rota_v1_RedriveDeadLetterRequest, callback: grpc.requestCallback<_rota_v1_RedriveDeadLetterResponse__Output>): grpc.ClientUnaryCall;
  redriveDeadLetter(argument: _rota_v1_RedriveDeadLetterRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_RedriveDeadLetterResponse__Output>): grpc.ClientUnaryCall;
  redriveDeadLetter(argument: _rota_v1_RedriveDeadLetterRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_RedriveDeadLetterResponse__Output>): grpc.ClientUnaryCall;
  redriveDeadLetter(argument: _rota_v1_RedriveDeadLetterRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_RedriveDeadLetterResponse__Output>): grpc.ClientUnaryCall;
  redriveDeadLetter(argument: _rota_v1_RedriveDeadLetterRequest, callback: grpc.requestCallback<_rota_v1_RedriveDeadLetterResponse__Output>): grpc.ClientUnaryCall;
  
  ReleaseSingletonLease(argument: _rota_v1_ReleaseSingletonRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_SingletonOpResult__Output>): grpc.ClientUnaryCall;
  ReleaseSingletonLease(argument: _rota_v1_ReleaseSingletonRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_SingletonOpResult__Output>): grpc.ClientUnaryCall;
  ReleaseSingletonLease(argument: _rota_v1_ReleaseSingletonRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_SingletonOpResult__Output>): grpc.ClientUnaryCall;
  ReleaseSingletonLease(argument: _rota_v1_ReleaseSingletonRequest, callback: grpc.requestCallback<_rota_v1_SingletonOpResult__Output>): grpc.ClientUnaryCall;
  releaseSingletonLease(argument: _rota_v1_ReleaseSingletonRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_SingletonOpResult__Output>): grpc.ClientUnaryCall;
  releaseSingletonLease(argument: _rota_v1_ReleaseSingletonRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_SingletonOpResult__Output>): grpc.ClientUnaryCall;
  releaseSingletonLease(argument: _rota_v1_ReleaseSingletonRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_SingletonOpResult__Output>): grpc.ClientUnaryCall;
  releaseSingletonLease(argument: _rota_v1_ReleaseSingletonRequest, callback: grpc.requestCallback<_rota_v1_SingletonOpResult__Output>): grpc.ClientUnaryCall;
  
  RenewSingletonLease(argument: _rota_v1_RenewSingletonRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_SingletonLease__Output>): grpc.ClientUnaryCall;
  RenewSingletonLease(argument: _rota_v1_RenewSingletonRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_SingletonLease__Output>): grpc.ClientUnaryCall;
  RenewSingletonLease(argument: _rota_v1_RenewSingletonRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_SingletonLease__Output>): grpc.ClientUnaryCall;
  RenewSingletonLease(argument: _rota_v1_RenewSingletonRequest, callback: grpc.requestCallback<_rota_v1_SingletonLease__Output>): grpc.ClientUnaryCall;
  renewSingletonLease(argument: _rota_v1_RenewSingletonRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_SingletonLease__Output>): grpc.ClientUnaryCall;
  renewSingletonLease(argument: _rota_v1_RenewSingletonRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_SingletonLease__Output>): grpc.ClientUnaryCall;
  renewSingletonLease(argument: _rota_v1_RenewSingletonRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_SingletonLease__Output>): grpc.ClientUnaryCall;
  renewSingletonLease(argument: _rota_v1_RenewSingletonRequest, callback: grpc.requestCallback<_rota_v1_SingletonLease__Output>): grpc.ClientUnaryCall;
  
  ResumeGroup(argument: _rota_v1_GroupRef, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_GroupConfig__Output>): grpc.ClientUnaryCall;
  ResumeGroup(argument: _rota_v1_GroupRef, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_GroupConfig__Output>): grpc.ClientUnaryCall;
  ResumeGroup(argument: _rota_v1_GroupRef, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_GroupConfig__Output>): grpc.ClientUnaryCall;
  ResumeGroup(argument: _rota_v1_GroupRef, callback: grpc.requestCallback<_rota_v1_GroupConfig__Output>): grpc.ClientUnaryCall;
  resumeGroup(argument: _rota_v1_GroupRef, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_GroupConfig__Output>): grpc.ClientUnaryCall;
  resumeGroup(argument: _rota_v1_GroupRef, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_GroupConfig__Output>): grpc.ClientUnaryCall;
  resumeGroup(argument: _rota_v1_GroupRef, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_GroupConfig__Output>): grpc.ClientUnaryCall;
  resumeGroup(argument: _rota_v1_GroupRef, callback: grpc.requestCallback<_rota_v1_GroupConfig__Output>): grpc.ClientUnaryCall;
  
  ResumeLane(argument: _rota_v1_LaneRef, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_LaneOpResult__Output>): grpc.ClientUnaryCall;
  ResumeLane(argument: _rota_v1_LaneRef, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_LaneOpResult__Output>): grpc.ClientUnaryCall;
  ResumeLane(argument: _rota_v1_LaneRef, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_LaneOpResult__Output>): grpc.ClientUnaryCall;
  ResumeLane(argument: _rota_v1_LaneRef, callback: grpc.requestCallback<_rota_v1_LaneOpResult__Output>): grpc.ClientUnaryCall;
  resumeLane(argument: _rota_v1_LaneRef, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_LaneOpResult__Output>): grpc.ClientUnaryCall;
  resumeLane(argument: _rota_v1_LaneRef, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_LaneOpResult__Output>): grpc.ClientUnaryCall;
  resumeLane(argument: _rota_v1_LaneRef, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_LaneOpResult__Output>): grpc.ClientUnaryCall;
  resumeLane(argument: _rota_v1_LaneRef, callback: grpc.requestCallback<_rota_v1_LaneOpResult__Output>): grpc.ClientUnaryCall;
  
  ScheduleCron(argument: _rota_v1_ScheduleCronRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_CronInfo__Output>): grpc.ClientUnaryCall;
  ScheduleCron(argument: _rota_v1_ScheduleCronRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_CronInfo__Output>): grpc.ClientUnaryCall;
  ScheduleCron(argument: _rota_v1_ScheduleCronRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_CronInfo__Output>): grpc.ClientUnaryCall;
  ScheduleCron(argument: _rota_v1_ScheduleCronRequest, callback: grpc.requestCallback<_rota_v1_CronInfo__Output>): grpc.ClientUnaryCall;
  scheduleCron(argument: _rota_v1_ScheduleCronRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_CronInfo__Output>): grpc.ClientUnaryCall;
  scheduleCron(argument: _rota_v1_ScheduleCronRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_CronInfo__Output>): grpc.ClientUnaryCall;
  scheduleCron(argument: _rota_v1_ScheduleCronRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_CronInfo__Output>): grpc.ClientUnaryCall;
  scheduleCron(argument: _rota_v1_ScheduleCronRequest, callback: grpc.requestCallback<_rota_v1_CronInfo__Output>): grpc.ClientUnaryCall;
  
  SetGroupConfig(argument: _rota_v1_SetGroupConfigRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_GroupConfig__Output>): grpc.ClientUnaryCall;
  SetGroupConfig(argument: _rota_v1_SetGroupConfigRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_GroupConfig__Output>): grpc.ClientUnaryCall;
  SetGroupConfig(argument: _rota_v1_SetGroupConfigRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_GroupConfig__Output>): grpc.ClientUnaryCall;
  SetGroupConfig(argument: _rota_v1_SetGroupConfigRequest, callback: grpc.requestCallback<_rota_v1_GroupConfig__Output>): grpc.ClientUnaryCall;
  setGroupConfig(argument: _rota_v1_SetGroupConfigRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_GroupConfig__Output>): grpc.ClientUnaryCall;
  setGroupConfig(argument: _rota_v1_SetGroupConfigRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_GroupConfig__Output>): grpc.ClientUnaryCall;
  setGroupConfig(argument: _rota_v1_SetGroupConfigRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_GroupConfig__Output>): grpc.ClientUnaryCall;
  setGroupConfig(argument: _rota_v1_SetGroupConfigRequest, callback: grpc.requestCallback<_rota_v1_GroupConfig__Output>): grpc.ClientUnaryCall;
  
  SetLaneConfig(argument: _rota_v1_SetLaneConfigRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_LaneConfig__Output>): grpc.ClientUnaryCall;
  SetLaneConfig(argument: _rota_v1_SetLaneConfigRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_LaneConfig__Output>): grpc.ClientUnaryCall;
  SetLaneConfig(argument: _rota_v1_SetLaneConfigRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_LaneConfig__Output>): grpc.ClientUnaryCall;
  SetLaneConfig(argument: _rota_v1_SetLaneConfigRequest, callback: grpc.requestCallback<_rota_v1_LaneConfig__Output>): grpc.ClientUnaryCall;
  setLaneConfig(argument: _rota_v1_SetLaneConfigRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_LaneConfig__Output>): grpc.ClientUnaryCall;
  setLaneConfig(argument: _rota_v1_SetLaneConfigRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_LaneConfig__Output>): grpc.ClientUnaryCall;
  setLaneConfig(argument: _rota_v1_SetLaneConfigRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_LaneConfig__Output>): grpc.ClientUnaryCall;
  setLaneConfig(argument: _rota_v1_SetLaneConfigRequest, callback: grpc.requestCallback<_rota_v1_LaneConfig__Output>): grpc.ClientUnaryCall;
  
  SetPolicy(argument: _rota_v1_SetPolicyRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_PolicyInfo__Output>): grpc.ClientUnaryCall;
  SetPolicy(argument: _rota_v1_SetPolicyRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_PolicyInfo__Output>): grpc.ClientUnaryCall;
  SetPolicy(argument: _rota_v1_SetPolicyRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_PolicyInfo__Output>): grpc.ClientUnaryCall;
  SetPolicy(argument: _rota_v1_SetPolicyRequest, callback: grpc.requestCallback<_rota_v1_PolicyInfo__Output>): grpc.ClientUnaryCall;
  setPolicy(argument: _rota_v1_SetPolicyRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_PolicyInfo__Output>): grpc.ClientUnaryCall;
  setPolicy(argument: _rota_v1_SetPolicyRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_PolicyInfo__Output>): grpc.ClientUnaryCall;
  setPolicy(argument: _rota_v1_SetPolicyRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_PolicyInfo__Output>): grpc.ClientUnaryCall;
  setPolicy(argument: _rota_v1_SetPolicyRequest, callback: grpc.requestCallback<_rota_v1_PolicyInfo__Output>): grpc.ClientUnaryCall;
  
  TeardownGroup(argument: _rota_v1_TeardownRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_TeardownResult__Output>): grpc.ClientUnaryCall;
  TeardownGroup(argument: _rota_v1_TeardownRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_TeardownResult__Output>): grpc.ClientUnaryCall;
  TeardownGroup(argument: _rota_v1_TeardownRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_TeardownResult__Output>): grpc.ClientUnaryCall;
  TeardownGroup(argument: _rota_v1_TeardownRequest, callback: grpc.requestCallback<_rota_v1_TeardownResult__Output>): grpc.ClientUnaryCall;
  teardownGroup(argument: _rota_v1_TeardownRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_TeardownResult__Output>): grpc.ClientUnaryCall;
  teardownGroup(argument: _rota_v1_TeardownRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_TeardownResult__Output>): grpc.ClientUnaryCall;
  teardownGroup(argument: _rota_v1_TeardownRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_TeardownResult__Output>): grpc.ClientUnaryCall;
  teardownGroup(argument: _rota_v1_TeardownRequest, callback: grpc.requestCallback<_rota_v1_TeardownResult__Output>): grpc.ClientUnaryCall;
  
  ValidatePolicy(argument: _rota_v1_SetPolicyRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_ValidatePolicyResult__Output>): grpc.ClientUnaryCall;
  ValidatePolicy(argument: _rota_v1_SetPolicyRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_ValidatePolicyResult__Output>): grpc.ClientUnaryCall;
  ValidatePolicy(argument: _rota_v1_SetPolicyRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_ValidatePolicyResult__Output>): grpc.ClientUnaryCall;
  ValidatePolicy(argument: _rota_v1_SetPolicyRequest, callback: grpc.requestCallback<_rota_v1_ValidatePolicyResult__Output>): grpc.ClientUnaryCall;
  validatePolicy(argument: _rota_v1_SetPolicyRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_ValidatePolicyResult__Output>): grpc.ClientUnaryCall;
  validatePolicy(argument: _rota_v1_SetPolicyRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_ValidatePolicyResult__Output>): grpc.ClientUnaryCall;
  validatePolicy(argument: _rota_v1_SetPolicyRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_ValidatePolicyResult__Output>): grpc.ClientUnaryCall;
  validatePolicy(argument: _rota_v1_SetPolicyRequest, callback: grpc.requestCallback<_rota_v1_ValidatePolicyResult__Output>): grpc.ClientUnaryCall;
  
}

export interface ControlHandlers extends grpc.UntypedServiceImplementation {
  AcquireSingletonLease: grpc.handleUnaryCall<_rota_v1_AcquireSingletonRequest__Output, _rota_v1_SingletonLease>;
  
  CancelGroup: grpc.handleUnaryCall<_rota_v1_GroupRef__Output, _rota_v1_GroupOpResult>;
  
  CompleteByToken: grpc.handleUnaryCall<_rota_v1_CompleteByTokenRequest__Output, _rota_v1_CompleteResult>;
  
  DeleteCron: grpc.handleUnaryCall<_rota_v1_CronRef__Output, _rota_v1_CronOpResult>;
  
  DescribeCluster: grpc.handleUnaryCall<_rota_v1_DescribeClusterRequest__Output, _rota_v1_ClusterInfo>;
  
  GetGroupConfig: grpc.handleUnaryCall<_rota_v1_GroupRef__Output, _rota_v1_GroupConfig>;
  
  GetLaneFairness: grpc.handleUnaryCall<_rota_v1_LaneRef__Output, _rota_v1_LaneFairness>;
  
  GetPolicy: grpc.handleUnaryCall<_rota_v1_LaneRef__Output, _rota_v1_PolicyInfo>;
  
  GetPolicyHealth: grpc.handleUnaryCall<_rota_v1_LaneRef__Output, _rota_v1_PolicyHealth>;
  
  GetStats: grpc.handleUnaryCall<_rota_v1_GetStatsRequest__Output, _rota_v1_StatsResponse>;
  
  Health: grpc.handleUnaryCall<_rota_v1_HealthRequest__Output, _rota_v1_HealthResponse>;
  
  ListCron: grpc.handleUnaryCall<_rota_v1_ListCronRequest__Output, _rota_v1_ListCronResponse>;
  
  ListDeadLetters: grpc.handleUnaryCall<_rota_v1_ListDeadLettersRequest__Output, _rota_v1_ListDeadLettersResponse>;
  
  ListGroups: grpc.handleUnaryCall<_rota_v1_ListGroupsRequest__Output, _rota_v1_ListGroupsResponse>;
  
  ListLeases: grpc.handleUnaryCall<_rota_v1_ListLeasesRequest__Output, _rota_v1_ListLeasesResponse>;
  
  PauseCron: grpc.handleUnaryCall<_rota_v1_CronRef__Output, _rota_v1_CronInfo>;
  
  PauseGroup: grpc.handleUnaryCall<_rota_v1_GroupRef__Output, _rota_v1_GroupConfig>;
  
  PauseLane: grpc.handleUnaryCall<_rota_v1_PauseLaneRequest__Output, _rota_v1_LaneOpResult>;
  
  PeekMessages: grpc.handleUnaryCall<_rota_v1_PeekMessagesRequest__Output, _rota_v1_PeekMessagesResponse>;
  
  PurgeGroup: grpc.handleUnaryCall<_rota_v1_GroupRef__Output, _rota_v1_GroupOpResult>;
  
  ReapGroup: grpc.handleUnaryCall<_rota_v1_GroupRef__Output, _rota_v1_GroupOpResult>;
  
  RedriveDeadLetter: grpc.handleUnaryCall<_rota_v1_RedriveDeadLetterRequest__Output, _rota_v1_RedriveDeadLetterResponse>;
  
  ReleaseSingletonLease: grpc.handleUnaryCall<_rota_v1_ReleaseSingletonRequest__Output, _rota_v1_SingletonOpResult>;
  
  RenewSingletonLease: grpc.handleUnaryCall<_rota_v1_RenewSingletonRequest__Output, _rota_v1_SingletonLease>;
  
  ResumeGroup: grpc.handleUnaryCall<_rota_v1_GroupRef__Output, _rota_v1_GroupConfig>;
  
  ResumeLane: grpc.handleUnaryCall<_rota_v1_LaneRef__Output, _rota_v1_LaneOpResult>;
  
  ScheduleCron: grpc.handleUnaryCall<_rota_v1_ScheduleCronRequest__Output, _rota_v1_CronInfo>;
  
  SetGroupConfig: grpc.handleUnaryCall<_rota_v1_SetGroupConfigRequest__Output, _rota_v1_GroupConfig>;
  
  SetLaneConfig: grpc.handleUnaryCall<_rota_v1_SetLaneConfigRequest__Output, _rota_v1_LaneConfig>;
  
  SetPolicy: grpc.handleUnaryCall<_rota_v1_SetPolicyRequest__Output, _rota_v1_PolicyInfo>;
  
  TeardownGroup: grpc.handleUnaryCall<_rota_v1_TeardownRequest__Output, _rota_v1_TeardownResult>;
  
  ValidatePolicy: grpc.handleUnaryCall<_rota_v1_SetPolicyRequest__Output, _rota_v1_ValidatePolicyResult>;
  
}

export interface ControlDefinition extends grpc.ServiceDefinition {
  AcquireSingletonLease: MethodDefinition<_rota_v1_AcquireSingletonRequest, _rota_v1_SingletonLease, _rota_v1_AcquireSingletonRequest__Output, _rota_v1_SingletonLease__Output>
  CancelGroup: MethodDefinition<_rota_v1_GroupRef, _rota_v1_GroupOpResult, _rota_v1_GroupRef__Output, _rota_v1_GroupOpResult__Output>
  CompleteByToken: MethodDefinition<_rota_v1_CompleteByTokenRequest, _rota_v1_CompleteResult, _rota_v1_CompleteByTokenRequest__Output, _rota_v1_CompleteResult__Output>
  DeleteCron: MethodDefinition<_rota_v1_CronRef, _rota_v1_CronOpResult, _rota_v1_CronRef__Output, _rota_v1_CronOpResult__Output>
  DescribeCluster: MethodDefinition<_rota_v1_DescribeClusterRequest, _rota_v1_ClusterInfo, _rota_v1_DescribeClusterRequest__Output, _rota_v1_ClusterInfo__Output>
  GetGroupConfig: MethodDefinition<_rota_v1_GroupRef, _rota_v1_GroupConfig, _rota_v1_GroupRef__Output, _rota_v1_GroupConfig__Output>
  GetLaneFairness: MethodDefinition<_rota_v1_LaneRef, _rota_v1_LaneFairness, _rota_v1_LaneRef__Output, _rota_v1_LaneFairness__Output>
  GetPolicy: MethodDefinition<_rota_v1_LaneRef, _rota_v1_PolicyInfo, _rota_v1_LaneRef__Output, _rota_v1_PolicyInfo__Output>
  GetPolicyHealth: MethodDefinition<_rota_v1_LaneRef, _rota_v1_PolicyHealth, _rota_v1_LaneRef__Output, _rota_v1_PolicyHealth__Output>
  GetStats: MethodDefinition<_rota_v1_GetStatsRequest, _rota_v1_StatsResponse, _rota_v1_GetStatsRequest__Output, _rota_v1_StatsResponse__Output>
  Health: MethodDefinition<_rota_v1_HealthRequest, _rota_v1_HealthResponse, _rota_v1_HealthRequest__Output, _rota_v1_HealthResponse__Output>
  ListCron: MethodDefinition<_rota_v1_ListCronRequest, _rota_v1_ListCronResponse, _rota_v1_ListCronRequest__Output, _rota_v1_ListCronResponse__Output>
  ListDeadLetters: MethodDefinition<_rota_v1_ListDeadLettersRequest, _rota_v1_ListDeadLettersResponse, _rota_v1_ListDeadLettersRequest__Output, _rota_v1_ListDeadLettersResponse__Output>
  ListGroups: MethodDefinition<_rota_v1_ListGroupsRequest, _rota_v1_ListGroupsResponse, _rota_v1_ListGroupsRequest__Output, _rota_v1_ListGroupsResponse__Output>
  ListLeases: MethodDefinition<_rota_v1_ListLeasesRequest, _rota_v1_ListLeasesResponse, _rota_v1_ListLeasesRequest__Output, _rota_v1_ListLeasesResponse__Output>
  PauseCron: MethodDefinition<_rota_v1_CronRef, _rota_v1_CronInfo, _rota_v1_CronRef__Output, _rota_v1_CronInfo__Output>
  PauseGroup: MethodDefinition<_rota_v1_GroupRef, _rota_v1_GroupConfig, _rota_v1_GroupRef__Output, _rota_v1_GroupConfig__Output>
  PauseLane: MethodDefinition<_rota_v1_PauseLaneRequest, _rota_v1_LaneOpResult, _rota_v1_PauseLaneRequest__Output, _rota_v1_LaneOpResult__Output>
  PeekMessages: MethodDefinition<_rota_v1_PeekMessagesRequest, _rota_v1_PeekMessagesResponse, _rota_v1_PeekMessagesRequest__Output, _rota_v1_PeekMessagesResponse__Output>
  PurgeGroup: MethodDefinition<_rota_v1_GroupRef, _rota_v1_GroupOpResult, _rota_v1_GroupRef__Output, _rota_v1_GroupOpResult__Output>
  ReapGroup: MethodDefinition<_rota_v1_GroupRef, _rota_v1_GroupOpResult, _rota_v1_GroupRef__Output, _rota_v1_GroupOpResult__Output>
  RedriveDeadLetter: MethodDefinition<_rota_v1_RedriveDeadLetterRequest, _rota_v1_RedriveDeadLetterResponse, _rota_v1_RedriveDeadLetterRequest__Output, _rota_v1_RedriveDeadLetterResponse__Output>
  ReleaseSingletonLease: MethodDefinition<_rota_v1_ReleaseSingletonRequest, _rota_v1_SingletonOpResult, _rota_v1_ReleaseSingletonRequest__Output, _rota_v1_SingletonOpResult__Output>
  RenewSingletonLease: MethodDefinition<_rota_v1_RenewSingletonRequest, _rota_v1_SingletonLease, _rota_v1_RenewSingletonRequest__Output, _rota_v1_SingletonLease__Output>
  ResumeGroup: MethodDefinition<_rota_v1_GroupRef, _rota_v1_GroupConfig, _rota_v1_GroupRef__Output, _rota_v1_GroupConfig__Output>
  ResumeLane: MethodDefinition<_rota_v1_LaneRef, _rota_v1_LaneOpResult, _rota_v1_LaneRef__Output, _rota_v1_LaneOpResult__Output>
  ScheduleCron: MethodDefinition<_rota_v1_ScheduleCronRequest, _rota_v1_CronInfo, _rota_v1_ScheduleCronRequest__Output, _rota_v1_CronInfo__Output>
  SetGroupConfig: MethodDefinition<_rota_v1_SetGroupConfigRequest, _rota_v1_GroupConfig, _rota_v1_SetGroupConfigRequest__Output, _rota_v1_GroupConfig__Output>
  SetLaneConfig: MethodDefinition<_rota_v1_SetLaneConfigRequest, _rota_v1_LaneConfig, _rota_v1_SetLaneConfigRequest__Output, _rota_v1_LaneConfig__Output>
  SetPolicy: MethodDefinition<_rota_v1_SetPolicyRequest, _rota_v1_PolicyInfo, _rota_v1_SetPolicyRequest__Output, _rota_v1_PolicyInfo__Output>
  TeardownGroup: MethodDefinition<_rota_v1_TeardownRequest, _rota_v1_TeardownResult, _rota_v1_TeardownRequest__Output, _rota_v1_TeardownResult__Output>
  ValidatePolicy: MethodDefinition<_rota_v1_SetPolicyRequest, _rota_v1_ValidatePolicyResult, _rota_v1_SetPolicyRequest__Output, _rota_v1_ValidatePolicyResult__Output>
}
