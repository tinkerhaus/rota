// Original file: proto/rota/v1/rota.proto

import type * as grpc from '@grpc/grpc-js'
import type { MethodDefinition } from '@grpc/proto-loader'
import type { PublishBatchRequest as _rota_v1_PublishBatchRequest, PublishBatchRequest__Output as _rota_v1_PublishBatchRequest__Output } from '../../rota/v1/PublishBatchRequest';
import type { PublishBatchResponse as _rota_v1_PublishBatchResponse, PublishBatchResponse__Output as _rota_v1_PublishBatchResponse__Output } from '../../rota/v1/PublishBatchResponse';
import type { PublishRequest as _rota_v1_PublishRequest, PublishRequest__Output as _rota_v1_PublishRequest__Output } from '../../rota/v1/PublishRequest';
import type { PublishResponse as _rota_v1_PublishResponse, PublishResponse__Output as _rota_v1_PublishResponse__Output } from '../../rota/v1/PublishResponse';
import type { WorkClientMsg as _rota_v1_WorkClientMsg, WorkClientMsg__Output as _rota_v1_WorkClientMsg__Output } from '../../rota/v1/WorkClientMsg';
import type { WorkServerMsg as _rota_v1_WorkServerMsg, WorkServerMsg__Output as _rota_v1_WorkServerMsg__Output } from '../../rota/v1/WorkServerMsg';

export interface BrokerClient extends grpc.Client {
  Publish(argument: _rota_v1_PublishRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_PublishResponse__Output>): grpc.ClientUnaryCall;
  Publish(argument: _rota_v1_PublishRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_PublishResponse__Output>): grpc.ClientUnaryCall;
  Publish(argument: _rota_v1_PublishRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_PublishResponse__Output>): grpc.ClientUnaryCall;
  Publish(argument: _rota_v1_PublishRequest, callback: grpc.requestCallback<_rota_v1_PublishResponse__Output>): grpc.ClientUnaryCall;
  publish(argument: _rota_v1_PublishRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_PublishResponse__Output>): grpc.ClientUnaryCall;
  publish(argument: _rota_v1_PublishRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_PublishResponse__Output>): grpc.ClientUnaryCall;
  publish(argument: _rota_v1_PublishRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_PublishResponse__Output>): grpc.ClientUnaryCall;
  publish(argument: _rota_v1_PublishRequest, callback: grpc.requestCallback<_rota_v1_PublishResponse__Output>): grpc.ClientUnaryCall;
  
  PublishBatch(argument: _rota_v1_PublishBatchRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_PublishBatchResponse__Output>): grpc.ClientUnaryCall;
  PublishBatch(argument: _rota_v1_PublishBatchRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_PublishBatchResponse__Output>): grpc.ClientUnaryCall;
  PublishBatch(argument: _rota_v1_PublishBatchRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_PublishBatchResponse__Output>): grpc.ClientUnaryCall;
  PublishBatch(argument: _rota_v1_PublishBatchRequest, callback: grpc.requestCallback<_rota_v1_PublishBatchResponse__Output>): grpc.ClientUnaryCall;
  publishBatch(argument: _rota_v1_PublishBatchRequest, metadata: grpc.Metadata, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_PublishBatchResponse__Output>): grpc.ClientUnaryCall;
  publishBatch(argument: _rota_v1_PublishBatchRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<_rota_v1_PublishBatchResponse__Output>): grpc.ClientUnaryCall;
  publishBatch(argument: _rota_v1_PublishBatchRequest, options: grpc.CallOptions, callback: grpc.requestCallback<_rota_v1_PublishBatchResponse__Output>): grpc.ClientUnaryCall;
  publishBatch(argument: _rota_v1_PublishBatchRequest, callback: grpc.requestCallback<_rota_v1_PublishBatchResponse__Output>): grpc.ClientUnaryCall;
  
  Work(metadata: grpc.Metadata, options?: grpc.CallOptions): grpc.ClientDuplexStream<_rota_v1_WorkClientMsg, _rota_v1_WorkServerMsg__Output>;
  Work(options?: grpc.CallOptions): grpc.ClientDuplexStream<_rota_v1_WorkClientMsg, _rota_v1_WorkServerMsg__Output>;
  work(metadata: grpc.Metadata, options?: grpc.CallOptions): grpc.ClientDuplexStream<_rota_v1_WorkClientMsg, _rota_v1_WorkServerMsg__Output>;
  work(options?: grpc.CallOptions): grpc.ClientDuplexStream<_rota_v1_WorkClientMsg, _rota_v1_WorkServerMsg__Output>;
  
}

export interface BrokerHandlers extends grpc.UntypedServiceImplementation {
  Publish: grpc.handleUnaryCall<_rota_v1_PublishRequest__Output, _rota_v1_PublishResponse>;
  
  PublishBatch: grpc.handleUnaryCall<_rota_v1_PublishBatchRequest__Output, _rota_v1_PublishBatchResponse>;
  
  Work: grpc.handleBidiStreamingCall<_rota_v1_WorkClientMsg__Output, _rota_v1_WorkServerMsg>;
  
}

export interface BrokerDefinition extends grpc.ServiceDefinition {
  Publish: MethodDefinition<_rota_v1_PublishRequest, _rota_v1_PublishResponse, _rota_v1_PublishRequest__Output, _rota_v1_PublishResponse__Output>
  PublishBatch: MethodDefinition<_rota_v1_PublishBatchRequest, _rota_v1_PublishBatchResponse, _rota_v1_PublishBatchRequest__Output, _rota_v1_PublishBatchResponse__Output>
  Work: MethodDefinition<_rota_v1_WorkClientMsg, _rota_v1_WorkServerMsg, _rota_v1_WorkClientMsg__Output, _rota_v1_WorkServerMsg__Output>
}
