package localManager

import (
	"github.com/dragonflyoss/Dragonfly/dfget/core/api"
	"github.com/dragonflyoss/Dragonfly/dfget/types"
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
