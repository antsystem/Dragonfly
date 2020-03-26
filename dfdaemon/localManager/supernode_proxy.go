package localManager

import (
	"github.com/dragonflyoss/Dragonfly/dfget/core/api"
	"github.com/dragonflyoss/Dragonfly/dfget/types"
	"github.com/dragonflyoss/Dragonfly/pkg/constants"
)

type superNodeProxy struct {
	nodes 		[]string
	api			api.SupernodeAPI
}

func newSuperNodeProxy(nodes []string) *superNodeProxy {
	return &superNodeProxy{
		nodes: nodes,
		api: api.NewSupernodeAPI(),
	}
}

func (proxy *superNodeProxy) ReportResource(req *types.RegisterRequest) (resp *types.RegisterResponse, e error) {
	for _, node := range proxy.nodes {
		resp, e = proxy.api.ReportResource(node, req)
		if e != nil {
			continue
		}

		if resp.Code != 200 {
			continue
		}
	}

	return
}

func (proxy *superNodeProxy) ReportResourceDeleted(taskID string, cid string) (resp *types.BaseResponse, e error) {
	for _, node := range proxy.nodes {
		resp, e = proxy.api.ReportResourceDeleted(node, taskID, cid)
		if e != nil {
			continue
		}

		if resp.Code != constants.CodeGetPeerDown {
			continue
		}
	}

	return
}
