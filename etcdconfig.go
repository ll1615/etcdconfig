package etcdconfig

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/coreos/etcd/mvcc/mvccpb"
	"go.etcd.io/etcd/clientv3"
)

// New returns etcd config client.
func New(endpoints []string) (*config, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: time.Second * 5,
	})

	if err != nil {
		return nil, err
	}

	return &config{kvs: make(map[string]string), cli: cli}, nil
}

type config struct {
	kvs map[string]string
	mu  sync.RWMutex

	cli *clientv3.Client
}

// Get returns the value and existence of the key.
func (c *config) Get(key string) (value string, exist bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	value, exist = c.kvs[key]
	return
}

// Load gets config from etcd and call callbacks.
func (c *config) Load(ctx context.Context, path string, callbacks map[string]func(string, string)) error {
	resp, err := c.cli.Get(ctx, path, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	c.mu.Lock()
	for _, kv := range resp.Kvs {
		c.kvs[string(kv.Key)] = string(kv.Value)
		if callback := callbacks[string(kv.Key)]; callback != nil {
			callback(string(kv.Key), string(kv.Value))
		}
	}
	c.mu.Unlock()

	return nil
}

// LoadAndWatch gets and watches config from etcd and call callbacks.
func (c *config) LoadAndWatch(ctx context.Context, path string, callbacks map[string]func(string, string)) error {
	if err := c.Load(ctx, path, callbacks); err != nil {
		return err
	}

	wc := c.cli.Watch(ctx, path, clientv3.WithPrefix())
	go func() {
		for wr := range wc {
			if err := wr.Err(); err != nil {
				log.Printf("watch %v failed: %v", path, err)
				return
			}

			c.mu.Lock()
			for _, evt := range wr.Events {
				switch evt.Type {
				case mvccpb.PUT:
					c.kvs[string(evt.Kv.Key)] = string(evt.Kv.Value)
					if callback := callbacks[string(evt.Kv.Key)]; callback != nil {
						callback(string(evt.Kv.Key), string(evt.Kv.Value))
					}
				case mvccpb.DELETE:
					delete(c.kvs, string(evt.Kv.Key))
				}
			}
			c.mu.Unlock()
		}
	}()

	return nil
}
