package ewatch

import (
	"sync"
	"sync/atomic"
	"time"

	// "context"
	"github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
	"go.etcd.io/etcd/clientv3"

	storagepb "github.com/coreos/etcd/storage/storagepb"
)

type Watcher interface {
	AddWatchKey(key string, list, notify func(...*storagepb.Event))
	DelWatchKey(key string)
}

type watcher struct {
	cli      *clientv3.Client
	watcher  clientv3.Watcher
	lsnLock  sync.RWMutex
	listener map[string]*notifier

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// 创建一个对象
func NewWatcher(cfg clientv3.Config) (Watcher, error) {
	cli, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}

	w := &watcher{
		cli:      cli,
		watcher:  cli.Watcher,
		listener: make(map[string]*notifier),
	}

	w.ctx, w.cancel = context.WithCancel(context.Background())

	go w.loopWatch()

	return w, nil
}

func (w *watcher) loopWatch() {
	for {
		<-w.ctx.Done() // 如果返回了

		w.lsnLock.Lock() // stw
		w.wg.Wait()

		w.watcher.Close()
		w.watcher = clientv3.NewWatcher(w.cli)
		w.cancel()
		w.ctx, w.cancel = context.WithCancel(context.Background())

		w.lsnLock.Unlock()

		go w.watchAll()
	}
}

func (w *watcher) watchAll() {
	w.lsnLock.RLock()
	if len(w.listener) == 0 {
		w.lsnLock.RUnlock()
		return
	}
	for key, no := range w.listener {
		w.wg.Add(1)
		go w.watchOne(key, no)
	}
	w.lsnLock.RUnlock()
}

func (w *watcher) watchOne(key string, no *notifier) {
	defer w.wg.Done()
	if !atomic.CompareAndSwapInt32(&no.status, 0, 1) {
		return
	}

	defer func() {
		atomic.CompareAndSwapInt32(&no.status, 1, 0)
	}()

	var events []*storagepb.Event

	for {
		res, err := w.cli.Get(w.ctx, key, clientv3.WithPrefix())
		if err != nil {
			select {
			case <-no.done:
				return
			case <-w.ctx.Done():
				return
			default:
				time.Sleep(time.Second * 3)
			}
			continue
		}

		for _, kv := range res.Kvs {
			events = append(events, &storagepb.Event{
				Type: storagepb.PUT,
				Kv:   kv,
			})
		}
		break
	}

	for _, list := range no.list {
		list(events...)
	}

	ww := w.watcher.Watch(w.ctx, key, clientv3.WithPrefix())
	for {
		select {
		case res, ok := <-ww:
			if res.Err() != nil || !ok || w.ctx.Err() != nil {
				w.cancel()
				return
			}

			for _, notify := range no.notifies {
				notify(res.Events...)
			}
		case <-no.done:
			return
		case <-w.ctx.Done():
			return
		}
	}
}

// 可以注册监听服务，如果变动有更新，并且包含回调
func (w *watcher) AddWatchKey(key string, list, notify func(...*storagepb.Event)) {
	w.lsnLock.RLock()
	no, ok := w.listener[key]
	w.lsnLock.RUnlock()
	if !ok { // 说明是新加入的监听
		w.lsnLock.Lock()
		no, ok = w.listener[key]
		if !ok {
			no = &notifier{
				done: make(chan struct{}),
			}
			w.listener[key] = no
			w.wg.Add(1)
			go w.watchOne(key, no)
		}
		w.lsnLock.Unlock()
	}
	no.noLock.Lock()
	no.list = append(no.list, list)
	no.notifies = append(no.notifies, notify)
	no.noLock.Unlock()
}

func (w *watcher) DelWatchKey(key string) {
	w.lsnLock.Lock()
	no, ok := w.listener[key]
	if !ok {
		w.lsnLock.Unlock()
		return
	}
	delete(w.listener, key)
	w.lsnLock.Unlock()
	no.close()
}
