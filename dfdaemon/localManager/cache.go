package localManager

import "github.com/dragonflyoss/Dragonfly/pkg/syncmap"

type cache struct {
	
}

type cacheManager struct {
	// key is taskID, value is the
	taskContainer	*syncmap.SyncMap
}