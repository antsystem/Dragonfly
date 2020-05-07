package scheduler

import (
	"github.com/dragonflyoss/Dragonfly/apis/types"
	"github.com/dragonflyoss/Dragonfly/pkg/errortypes"
	"github.com/dragonflyoss/Dragonfly/pkg/stringutils"
	"github.com/dragonflyoss/Dragonfly/pkg/syncmap"

	"github.com/pkg/errors"
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
		return nil, err
	}

	if ts, ok := v.(*taskState); ok {
		return ts, nil
	}

	return nil, errors.Wrapf(errortypes.ErrConvertFailed, "key %s: %v", key, v)
}

func (dm *dataMap) getAsNode(key string) (*types.Node, error) {
	if stringutils.IsEmptyStr(key) {
		return nil, errors.Wrap(errortypes.ErrEmptyValue, "taskID")
	}

	v, err := dm.Get(key)
	if err != nil {
		return nil, err
	}

	if ts, ok := v.(*types.Node); ok {
		return ts, nil
	}

	return nil, errors.Wrapf(errortypes.ErrConvertFailed, "key %s: %v", key, v)
}

func (dm *dataMap) getAsLocalTaskState(key string) (*localTaskState, error) {
	if stringutils.IsEmptyStr(key) {
		return nil, errors.Wrap(errortypes.ErrEmptyValue, "taskID")
	}

	v, err := dm.Get(key)
	if err != nil {
		return nil, err
	}

	if lts, ok := v.(*localTaskState); ok {
		return lts, nil
	}

	return nil, errors.Wrapf(errortypes.ErrConvertFailed, "key %s: %v", key, v)
}
