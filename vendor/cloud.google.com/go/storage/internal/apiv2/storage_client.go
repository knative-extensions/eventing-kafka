// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Code generated by protoc-gen-go_gapic. DO NOT EDIT.

package storage

import (
	"context"
	"math"
	"time"

	gax "github.com/googleapis/gax-go/v2"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"
	gtransport "google.golang.org/api/transport/grpc"
	storagepb "google.golang.org/genproto/googleapis/storage/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

var newClientHook clientHook

// CallOptions contains the retry settings for each method of Client.
type CallOptions struct {
	ReadObject          []gax.CallOption
	WriteObject         []gax.CallOption
	StartResumableWrite []gax.CallOption
	QueryWriteStatus    []gax.CallOption
}

func defaultGRPCClientOptions() []option.ClientOption {
	return []option.ClientOption{
		internaloption.WithDefaultEndpoint("storage.googleapis.com:443"),
		internaloption.WithDefaultMTLSEndpoint("storage.mtls.googleapis.com:443"),
		internaloption.WithDefaultAudience("https://storage.googleapis.com/"),
		internaloption.WithDefaultScopes(DefaultAuthScopes()...),
		internaloption.EnableJwtWithScope(),
		option.WithGRPCDialOption(grpc.WithDisableServiceConfig()),
		option.WithGRPCDialOption(grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(math.MaxInt32))),
	}
}

func defaultCallOptions() *CallOptions {
	return &CallOptions{
		ReadObject: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.DeadlineExceeded,
					codes.Unavailable,
				}, gax.Backoff{
					Initial:    1000 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 2.00,
				})
			}),
		},
		WriteObject: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.DeadlineExceeded,
					codes.Unavailable,
				}, gax.Backoff{
					Initial:    1000 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 2.00,
				})
			}),
		},
		StartResumableWrite: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.DeadlineExceeded,
					codes.Unavailable,
				}, gax.Backoff{
					Initial:    1000 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 2.00,
				})
			}),
		},
		QueryWriteStatus: []gax.CallOption{
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.DeadlineExceeded,
					codes.Unavailable,
				}, gax.Backoff{
					Initial:    1000 * time.Millisecond,
					Max:        60000 * time.Millisecond,
					Multiplier: 2.00,
				})
			}),
		},
	}
}

// internalClient is an interface that defines the methods availaible from Cloud Storage API.
type internalClient interface {
	Close() error
	setGoogleClientInfo(...string)
	Connection() *grpc.ClientConn
	ReadObject(context.Context, *storagepb.ReadObjectRequest, ...gax.CallOption) (storagepb.Storage_ReadObjectClient, error)
	WriteObject(context.Context, ...gax.CallOption) (storagepb.Storage_WriteObjectClient, error)
	StartResumableWrite(context.Context, *storagepb.StartResumableWriteRequest, ...gax.CallOption) (*storagepb.StartResumableWriteResponse, error)
	QueryWriteStatus(context.Context, *storagepb.QueryWriteStatusRequest, ...gax.CallOption) (*storagepb.QueryWriteStatusResponse, error)
}

// Client is a client for interacting with Cloud Storage API.
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
//
// Manages Google Cloud Storage resources.
type Client struct {
	// The internal transport-dependent client.
	internalClient internalClient

	// The call options for this service.
	CallOptions *CallOptions
}

// Wrapper methods routed to the internal client.

// Close closes the connection to the API service. The user should invoke this when
// the client is no longer required.
func (c *Client) Close() error {
	return c.internalClient.Close()
}

// setGoogleClientInfo sets the name and version of the application in
// the `x-goog-api-client` header passed on each request. Intended for
// use by Google-written clients.
func (c *Client) setGoogleClientInfo(keyval ...string) {
	c.internalClient.setGoogleClientInfo(keyval...)
}

// Connection returns a connection to the API service.
//
// Deprecated.
func (c *Client) Connection() *grpc.ClientConn {
	return c.internalClient.Connection()
}

// ReadObject reads an object’s data.
func (c *Client) ReadObject(ctx context.Context, req *storagepb.ReadObjectRequest, opts ...gax.CallOption) (storagepb.Storage_ReadObjectClient, error) {
	return c.internalClient.ReadObject(ctx, req, opts...)
}

// WriteObject stores a new object and metadata.
//
// An object can be written either in a single message stream or in a
// resumable sequence of message streams. To write using a single stream,
// the client should include in the first message of the stream an
// WriteObjectSpec describing the destination bucket, object, and any
// preconditions. Additionally, the final message must set ‘finish_write’ to
// true, or else it is an error.
//
// For a resumable write, the client should instead call
// StartResumableWrite() and provide that method an WriteObjectSpec.
// They should then attach the returned upload_id to the first message of
// each following call to Create. If there is an error or the connection is
// broken during the resumable Create(), the client should check the status
// of the Create() by calling QueryWriteStatus() and continue writing from
// the returned persisted_size. This may be less than the amount of data the
// client previously sent.
//
// The service will not view the object as complete until the client has
// sent a WriteObjectRequest with finish_write set to true. Sending any
// requests on a stream after sending a request with finish_write set to
// true will cause an error. The client should check the response it
// receives to determine how much data the service was able to commit and
// whether the service views the object as complete.
func (c *Client) WriteObject(ctx context.Context, opts ...gax.CallOption) (storagepb.Storage_WriteObjectClient, error) {
	return c.internalClient.WriteObject(ctx, opts...)
}

// StartResumableWrite starts a resumable write. How long the write operation remains valid, and
// what happens when the write operation becomes invalid, are
// service-dependent.
func (c *Client) StartResumableWrite(ctx context.Context, req *storagepb.StartResumableWriteRequest, opts ...gax.CallOption) (*storagepb.StartResumableWriteResponse, error) {
	return c.internalClient.StartResumableWrite(ctx, req, opts...)
}

// QueryWriteStatus determines the persisted_size for an object that is being written, which
// can then be used as the write_offset for the next Write() call.
//
// If the object does not exist (i.e., the object has been deleted, or the
// first Write() has not yet reached the service), this method returns the
// error NOT_FOUND.
//
// The client may call QueryWriteStatus() at any time to determine how
// much data has been processed for this object. This is useful if the
// client is buffering data and needs to know which data can be safely
// evicted. For any sequence of QueryWriteStatus() calls for a given
// object name, the sequence of returned persisted_size values will be
// non-decreasing.
func (c *Client) QueryWriteStatus(ctx context.Context, req *storagepb.QueryWriteStatusRequest, opts ...gax.CallOption) (*storagepb.QueryWriteStatusResponse, error) {
	return c.internalClient.QueryWriteStatus(ctx, req, opts...)
}

// gRPCClient is a client for interacting with Cloud Storage API over gRPC transport.
//
// Methods, except Close, may be called concurrently. However, fields must not be modified concurrently with method calls.
type gRPCClient struct {
	// Connection pool of gRPC connections to the service.
	connPool gtransport.ConnPool

	// flag to opt out of default deadlines via GOOGLE_API_GO_EXPERIMENTAL_DISABLE_DEFAULT_DEADLINE
	disableDeadlines bool

	// Points back to the CallOptions field of the containing Client
	CallOptions **CallOptions

	// The gRPC API client.
	client storagepb.StorageClient

	// The x-goog-* metadata to be sent with each request.
	xGoogMetadata metadata.MD
}

// NewClient creates a new storage client based on gRPC.
// The returned client must be Closed when it is done being used to clean up its underlying connections.
//
// Manages Google Cloud Storage resources.
func NewClient(ctx context.Context, opts ...option.ClientOption) (*Client, error) {
	clientOpts := defaultGRPCClientOptions()
	if newClientHook != nil {
		hookOpts, err := newClientHook(ctx, clientHookParams{})
		if err != nil {
			return nil, err
		}
		clientOpts = append(clientOpts, hookOpts...)
	}

	disableDeadlines, err := checkDisableDeadlines()
	if err != nil {
		return nil, err
	}

	connPool, err := gtransport.DialPool(ctx, append(clientOpts, opts...)...)
	if err != nil {
		return nil, err
	}
	client := Client{CallOptions: defaultCallOptions()}

	c := &gRPCClient{
		connPool:         connPool,
		disableDeadlines: disableDeadlines,
		client:           storagepb.NewStorageClient(connPool),
		CallOptions:      &client.CallOptions,
	}
	c.setGoogleClientInfo()

	client.internalClient = c

	return &client, nil
}

// Connection returns a connection to the API service.
//
// Deprecated.
func (c *gRPCClient) Connection() *grpc.ClientConn {
	return c.connPool.Conn()
}

// setGoogleClientInfo sets the name and version of the application in
// the `x-goog-api-client` header passed on each request. Intended for
// use by Google-written clients.
func (c *gRPCClient) setGoogleClientInfo(keyval ...string) {
	kv := append([]string{"gl-go", versionGo()}, keyval...)
	kv = append(kv, "gapic", versionClient, "gax", gax.Version, "grpc", grpc.Version)
	c.xGoogMetadata = metadata.Pairs("x-goog-api-client", gax.XGoogHeader(kv...))
}

// Close closes the connection to the API service. The user should invoke this when
// the client is no longer required.
func (c *gRPCClient) Close() error {
	return c.connPool.Close()
}

func (c *gRPCClient) ReadObject(ctx context.Context, req *storagepb.ReadObjectRequest, opts ...gax.CallOption) (storagepb.Storage_ReadObjectClient, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	var resp storagepb.Storage_ReadObjectClient
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.ReadObject(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *gRPCClient) WriteObject(ctx context.Context, opts ...gax.CallOption) (storagepb.Storage_WriteObjectClient, error) {
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	var resp storagepb.Storage_WriteObjectClient
	opts = append((*c.CallOptions).WriteObject[0:len((*c.CallOptions).WriteObject):len((*c.CallOptions).WriteObject)], opts...)
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.WriteObject(ctx, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *gRPCClient) StartResumableWrite(ctx context.Context, req *storagepb.StartResumableWriteRequest, opts ...gax.CallOption) (*storagepb.StartResumableWriteResponse, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append((*c.CallOptions).StartResumableWrite[0:len((*c.CallOptions).StartResumableWrite):len((*c.CallOptions).StartResumableWrite)], opts...)
	var resp *storagepb.StartResumableWriteResponse
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.StartResumableWrite(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *gRPCClient) QueryWriteStatus(ctx context.Context, req *storagepb.QueryWriteStatusRequest, opts ...gax.CallOption) (*storagepb.QueryWriteStatusResponse, error) {
	if _, ok := ctx.Deadline(); !ok && !c.disableDeadlines {
		cctx, cancel := context.WithTimeout(ctx, 60000*time.Millisecond)
		defer cancel()
		ctx = cctx
	}
	ctx = insertMetadata(ctx, c.xGoogMetadata)
	opts = append((*c.CallOptions).QueryWriteStatus[0:len((*c.CallOptions).QueryWriteStatus):len((*c.CallOptions).QueryWriteStatus)], opts...)
	var resp *storagepb.QueryWriteStatusResponse
	err := gax.Invoke(ctx, func(ctx context.Context, settings gax.CallSettings) error {
		var err error
		resp, err = c.client.QueryWriteStatus(ctx, req, settings.GRPC...)
		return err
	}, opts...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}
