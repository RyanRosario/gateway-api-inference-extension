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
	"net"
	"strings"
	"sync/atomic"

	extProcPb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sigs.k8s.io/controller-runtime/pkg/log"

	envoy "sigs.k8s.io/gateway-api-inference-extension/pkg/common/envoy"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/lwepp/datastore"
)

func (s *StreamingServer) handleRequestHeaders(ctx context.Context, reqCtx *RequestContext, req *extProcPb.ProcessingRequest_RequestHeaders) error {
	logger := log.FromContext(ctx)

	var filterEndpoints []string
	for _, header := range req.RequestHeaders.Headers.Headers {
		if header.Key == "test-epp-endpoint-selection" {
			val := envoy.GetHeaderValue(header)
			if val != "" {
				filterEndpoints = strings.Split(val, ",")
			}
		}
	}

	allPods := s.datastore.PodList(datastore.AllPodsPredicate)
	if len(allPods) == 0 {
		return status.Errorf(codes.Unavailable, "no pods available")
	}

	var candidates []*datastore.Endpoint
	if len(filterEndpoints) > 0 {
		for _, pod := range allPods {
			for _, filter := range filterEndpoints {
				if pod.Address == strings.TrimSpace(filter) {
					candidates = append(candidates, pod)
					break
				}
			}
		}
	}

	// If no matches or header not present, use all pods
	if len(candidates) == 0 {
		candidates = allPods
	}

	// Round-robin selection
	index := atomic.AddUint64(&s.rrIndex, 1)
	selectedPod := candidates[index%uint64(len(candidates))]

	reqCtx.SelectedPodIP = selectedPod.Address
	reqCtx.TargetEndpoint = net.JoinHostPort(selectedPod.Address, selectedPod.Port)

	logger.Info("Selected endpoint", "podIP", selectedPod.Address, "endpoint", reqCtx.TargetEndpoint)

	return nil
}
