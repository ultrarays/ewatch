package ewatch

import (
	"sync"

	storagepb "github.com/coreos/etcd/storage/storagepb"
)

type notifier struct {
	noLock   sync.RWMutex
	notifies []func(...*storagepb.Event)
	list     []func(...*storagepb.Event)
	done     chan struct{}
	status   int32
}

func (no *notifier) close() {
	close(no.done)
}
