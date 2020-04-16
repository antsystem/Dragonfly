package p2p

import (
	"context"
	"github.com/dragonflyoss/Dragonfly/dfdaemon/scheduler"
	"github.com/dragonflyoss/Dragonfly/dfget/core/api"
	"io"
)

// Manager control the
type Manager struct {
	sm 			 *scheduler.Manager
	supernodeAPI api.SupernodeAPI
	downloadAPI  api.DownloadAPI
	uploaderAPI  api.UploaderAPI
}

func (m *Manager) DownloadStreamContext(ctx context.Context, url string, header map[string][]string, name string) (io.Reader, error) {

}

