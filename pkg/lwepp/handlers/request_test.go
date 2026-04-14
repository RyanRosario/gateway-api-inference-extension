/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package handlers

import (
	"context"
	"testing"

	configPb "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extProcPb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/stretchr/testify/assert"

	"sigs.k8s.io/gateway-api-inference-extension/pkg/lwepp/datastore"
)

type mockDatastore struct {
	pods []*datastore.Endpoint
}

func (m *mockDatastore) PoolGet() (*datastore.EndpointPool, error) {
	return nil, nil
}

func (m *mockDatastore) PodList(predicate func(*datastore.Endpoint) bool) []*datastore.Endpoint {
	var res []*datastore.Endpoint
	for _, p := range m.pods {
		if predicate(p) {
			res = append(res, p)
		}
	}
	return res
}

func TestHandleRequestHeaders_RoundRobin(t *testing.T) {
	pods := []*datastore.Endpoint{
		{Address: "10.0.0.1", Port: "8080"},
		{Address: "10.0.0.2", Port: "8080"},
	}
	ds := &mockDatastore{pods: pods}
	server := &StreamingServer{datastore: ds}

	req := &extProcPb.ProcessingRequest_RequestHeaders{
		RequestHeaders: &extProcPb.HttpHeaders{
			Headers: &configPb.HeaderMap{},
		},
	}

	// First request
	reqCtx1 := &RequestContext{}
	err := server.handleRequestHeaders(context.Background(), reqCtx1, req)
	assert.NoError(t, err)

	// Second request
	reqCtx2 := &RequestContext{}
	err = server.handleRequestHeaders(context.Background(), reqCtx2, req)
	assert.NoError(t, err)

	// They should be different pods (round-robin)
	assert.NotEqual(t, reqCtx1.SelectedPodIP, reqCtx2.SelectedPodIP)

	// Third request should wrap around
	reqCtx3 := &RequestContext{}
	err = server.handleRequestHeaders(context.Background(), reqCtx3, req)
	assert.NoError(t, err)
	assert.Equal(t, reqCtx1.SelectedPodIP, reqCtx3.SelectedPodIP)
}

func TestHandleRequestHeaders_Filtering(t *testing.T) {
	pods := []*datastore.Endpoint{
		{Address: "10.0.0.1", Port: "8080"},
		{Address: "10.0.0.2", Port: "8080"},
		{Address: "10.0.0.3", Port: "8080"},
	}
	ds := &mockDatastore{pods: pods}
	server := &StreamingServer{datastore: ds}

	req := &extProcPb.ProcessingRequest_RequestHeaders{
		RequestHeaders: &extProcPb.HttpHeaders{
			Headers: &configPb.HeaderMap{
				Headers: []*configPb.HeaderValue{
					{Key: "test-epp-endpoint-selection", Value: "10.0.0.2"},
				},
			},
		},
	}

	reqCtx := &RequestContext{}
	err := server.handleRequestHeaders(context.Background(), reqCtx, req)
	assert.NoError(t, err)
	assert.Equal(t, "10.0.0.2", reqCtx.SelectedPodIP)
}

func TestHandleRequestHeaders_NoPods(t *testing.T) {
	ds := &mockDatastore{pods: []*datastore.Endpoint{}}
	server := &StreamingServer{datastore: ds}

	req := &extProcPb.ProcessingRequest_RequestHeaders{
		RequestHeaders: &extProcPb.HttpHeaders{
			Headers: &configPb.HeaderMap{},
		},
	}

	reqCtx := &RequestContext{}
	err := server.handleRequestHeaders(context.Background(), reqCtx, req)
	assert.Error(t, err)
}
