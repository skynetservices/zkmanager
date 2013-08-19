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
func TestConnect(t *testing.T) {
	eChan := make(chan zk.Event)
	zkconn := &TestConnection{}

	factory = func(servers []string, recvTimeout time.Duration) (ZkConnection, <-chan zk.Event, error) {
		return zkconn, eChan, nil
	}
}
