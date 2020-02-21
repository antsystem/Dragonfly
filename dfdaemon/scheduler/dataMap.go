package scheduler

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/dragonflyoss/Dragonfly/pkg/errortypes"
	"github.com/dragonflyoss/Dragonfly/pkg/stringutils"
	"github.com/dragonflyoss/Dragonfly/pkg/syncmap"
)

type dataMap struct {
	*syncmap.SyncMap
}

func newDataMap() *dataMap {
	return &dataMap{
		syncmap.NewSyncMap(),
	}
}

func (dm *dataMap) add(key string, value interface{}) error {
	return dm.Add(key, value)
}

func (dm *dataMap) remove(key string) error {
	return dm.Remove(key)
}

func (dm *dataMap) getAsTaskState(key string) (*taskState, error) {
	if stringutils.IsEmptyStr(key) {
		return nil, errors.Wrap(errortypes.ErrEmptyValue, "taskID")
	}

	v, err := dm.Get(key)
	if err != nil {
		return nil, errors.Wrapf(err, "key %s", key)
	}

	if ts, ok := v.(*taskState); ok {
		return ts, nil
	}

	return nil, errors.Wrapf(errortypes.ErrConvertFailed, "key %s: %v", key, v)
}
