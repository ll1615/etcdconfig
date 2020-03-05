package etcdconfig

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"go.etcd.io/etcd/clientv3"
)

func TestConfig(t *testing.T) {
	check := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}

	etcdAddr := os.Getenv("ETCD_ADDR")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// write config
	cli, err := clientv3.New(clientv3.Config{Endpoints: strings.Split(etcdAddr, ","), DialTimeout: time.Second * 3})
	check(err)

	lease := clientv3.NewLease(cli)
	lgr, err := lease.Grant(ctx, 10)
	check(err)

	key0 := "/test_key_0"
	value0 := "test_value_0"
	_, err = cli.Put(ctx, key0, value0, clientv3.WithLease(lgr.ID))
	check(err)

	// init config object
	c, err := New(strings.Split(etcdAddr, ","))
	check(err)

	t.Run("TestLoad", func(t *testing.T) {
		err = c.Load(ctx, "", map[string]func(string, string){
			key0: func(k, v string) {
				if v != value0 {
					t.Fatalf("key[%v] value, got: %v, got: %v", k, v, value0)
				}
			},
		})
		check(err)
	})

	t.Run("TestGet", func(t *testing.T) {
		value, exist := c.Get(key0)
		if !exist || value0 != value {
			t.Fatalf("get failed, value: %v, exist: %v", value, exist)
		}

		_, exist = c.Get(key0 + "1")
		if exist {
			t.Fatalf("got unexist key: %v1", key0)
		}
	})

	t.Run("TestLoadAndWatch", func(t *testing.T) {
		key1 := "/test_key_1"
		value1 := "test_value_1"
		err = c.LoadAndWatch(ctx, "", map[string]func(string, string){
			key0: func(k, v string) {
				if v != value0 {
					t.Fatalf("key[%v] value, got: %v, want: %v", k, v, value0)
				}
			},
			key1: func(k, v string) {
				if v != value1 {
					t.Fatalf("key[%v] value, got: %v, want: %v", k, v, value1)
				}
			},
		})
		check(err)

		_, err = cli.Put(ctx, key1, value1, clientv3.WithLease(lgr.ID))
		check(err)

		time.Sleep(1e8)
	})
}
