package scheduler

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/dragonflyoss/Dragonfly/pkg/errortypes"
	"github.com/dragonflyoss/Dragonfly/pkg/stringutils"
	"github.com/dragonflyoss/Dragonfly/pkg/syncmap"
)

type taskState struct {
	peerContainer	*syncmap.SyncMap
}

func newTaskState() *taskState {
	return &taskState{
		peerContainer: syncmap.NewSyncMap(),
	}
}

func (ts *taskState) add(peerID string, load int) error {
	if stringutils.IsEmptyStr(peerID) {
		return errors.Wrap(errortypes.ErrEmptyValue, "peerID")
	}

	ld, err := ts.peerContainer.GetAsInt(peerID)
	if err == nil && ld == load {
		logrus.Warnf("peerID: %s is exist", peerID)
		return nil
	}

	if err != nil && !errortypes.IsDataNotFound(err) {
		return err
	}

	return ps.pieceContainer.Add(peerID, true)
}
