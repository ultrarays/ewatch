package ewatch

import (
	"testing"
	"time"

	storagepb "github.com/coreos/etcd/storage/storagepb"
	"go.etcd.io/etcd/clientv3"
)

func TestWatch(t *testing.T) {
	cfg := clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: time.Second * 5,
	}
	w, err := NewWatcher(cfg)
	if err != nil {
		t.Fatalf("NewWatcher error (%+v)\n", err)
	}

	for _, v := range []string{"server", "name"} {
		w.AddWatchKey(v, func(v string) func(events ...*storagepb.Event) {
			return func(events ...*storagepb.Event) {
				t.Logf("%s list --- >", v)
				for _, event := range events {
					t.Logf("type:%s key:%s value:%s\n", event.Type.String(), string(event.Kv.Key), string(event.Kv.Value))
				}
			}
		}(v), func(v string) func(events ...*storagepb.Event) {
			return func(events ...*storagepb.Event) {
				t.Logf("%s watch --- >", v)
				for _, event := range events {
					t.Logf("type:%s key:%s value:%s\n", event.Type.String(), string(event.Kv.Key), string(event.Kv.Value))
				}
			}
		}(v))

	}

	select {}
}
