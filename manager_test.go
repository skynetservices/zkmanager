package zkmanager

import (
	"github.com/samuel/go-zookeeper/zk"
	"testing"
	"time"
)

// TODO: Test Shutdown
// TODO: Test Add
// TODO: Test Update
// TODO: Test Remove
// TODO: Test Register
// TODO: Test Unregister
// TODO: Test Watch
// TODO: Test Event notifications, nodes added/removed
// TODO: Test watches are put back in place if session expires
// TODO: Test cache building
// TODO: Test Connect

func TestCreatesDefaultPaths(t *testing.T) {
	multiCalled := false

	eChan := make(chan zk.Event)
	zkconn := &TestConnection{
		MultiFunc: func(ops zk.MultiOps) error {
			// TODO: should inspect these for a more valid test
			if len(ops.Create) != 4 {
				t.Fatal("Should be creating 4 paths when creating default paths")
			}

			multiCalled = true
			return nil
		},
	}

	factory = func(servers []string, recvTimeout time.Duration) (ZkConnection, <-chan zk.Event, error) {
		return zkconn, eChan, nil
	}

	NewZookeeperServiceManager("foo", 1*time.Second)

	if !multiCalled {
		t.Fatal("Did not create default paths")
	}
}
