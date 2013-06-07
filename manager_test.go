package zkmanager

import (
	"github.com/skynetservices/skynet2"
	"testing"
	"time"
)

func TestAddSubscriber(t *testing.T) {

	sm := NewZookeeperServiceManager("localhost:2181", 1*time.Second)
	l := len(subscribers)
	events := sm.Subscribe(skynet.ServiceQuery{})
	newl := len(subscribers)
	if newl <= l {
		t.Error("Subscriber Count Not Incremented")
	}
	close(events)

}
