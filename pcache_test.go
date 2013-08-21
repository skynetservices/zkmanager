package zkmanager

import (
	"bytes"
	"github.com/samuel/go-zookeeper/zk"
	"testing"
	"time"
)

// TODO: Test Start/Restart

func TestNewShouldWatchPath(t *testing.T) {
	ch := make(chan bool)
	pcs := newTestPathCache("/foo", []byte(""), 0)

	pcs.zkConn.GetWFunc = func(path string) (b []byte, st *zk.Stat, ev <-chan zk.Event, err error) {
		go func() {
			ch <- true
		}()

		return pcs.pathValue, pcs.pathStat, pcs.pathEvents, nil
	}

	pc, _, _ := NewPathCache(pcs.path, pcs.depth, pcs.serviceManager)
	defer pc.Stop()

	if !assertChannelReceived(ch, 10*time.Millisecond) {
		t.Fatal("GetW not called when creating PathCache")
	}
}

func TestUpdateSendsNotification(t *testing.T) {
	pcs := newTestPathCache("/foo", []byte(""), 0)
	defer pcs.cleanup()

	pc, events, _ := pcs.PathCache()
	defer pc.Stop()

	pcs.updateValue([]byte("Update1"))

	// Wait for event on Path before checking value
	ev := <-events

	if ev.Type != PathCacheUpdateNotification || ev.Path != pcs.path {
		t.Fatal("EventNodeDataChanged not propogated to parents")
	}
}

func TestShouldUpdateValueOnChange(t *testing.T) {
	pcs := newTestPathCache("/foo", []byte(""), 0)
	defer pcs.cleanup()

	pc, pathEvents, err := pcs.PathCache()
	defer pc.Stop()

	if err != nil {
		panic(err)
	}

	// Trying multiple times will ensure the watch is activated again
	values := []string{"foo", "bar", "baz"}

	for _, v := range values {
		pcs.pathValue = []byte(v)
		pcs.sendEvent(zk.EventNodeDataChanged, pcs.path)

		// Wait for event on Path before checking value
		<-pathEvents
		pcVal := pc.Value()

		if !bytes.Equal(pcVal, pcs.pathValue) {
			t.Fatal("PathCache did not update value:", string(pcVal), string(pcs.pathValue))
		}
	}
}

func TestDeletePathShouldNotifyParent(t *testing.T) {
	pcs := newTestPathCache("/foo", []byte(""), 0)
	defer pcs.cleanup()

	pc, events, _ := pcs.PathCache()
	defer pc.Stop()

	go pcs.sendEvent(zk.EventNodeDeleted, pcs.path)

	// Wait for event on Path before checking value
	ev := <-events

	if ev.Type != PathCacheRemoveNotification || ev.Path != pcs.path {
		t.Fatal("EventNodeDeleted not propogated to parents")
	}
}

func TestStop(t *testing.T) {
	pcs := newTestPathCache("/foo", []byte(""), 0)
	defer pcs.cleanup()

	pc, events, _ := pcs.PathCache()
	pc.Stop()

	_, ok := <-events

	if ok {
		t.Fatal("Stop should shutdown notification channel")
	}

	if len(pc.children) > 0 {
		t.Fatal("children not removed")
	}
}

func TestNewShouldWatchChildren(t *testing.T) {
	ch := make(chan bool)
	pcs := newTestPathCache("/foo", []byte(""), 1)
	defer pcs.cleanup()

	pcs.zkConn.ChildrenWFunc = func(path string) (children []string, st *zk.Stat, ev <-chan zk.Event, err error) {
		go func() {
			ch <- true
		}()

		return pcs.pathChildren, pcs.pathStat, pcs.pathEvents, nil
	}

	pc, _, _ := NewPathCache(pcs.path, pcs.depth, pcs.serviceManager)
	defer pc.Stop()

	if !assertChannelReceived(ch, 10*time.Millisecond) {
		t.Fatal("ChildrenW not called when creating PathCache with depth > 0")
	}
}

func TestShouldNotWatchChildrenIfDepthIsZero(t *testing.T) {
	ch := make(chan bool)
	pcs := newTestPathCache("/foo", []byte(""), 0)
	defer pcs.cleanup()

	pcs.zkConn.ChildrenWFunc = func(path string) (children []string, st *zk.Stat, ev <-chan zk.Event, err error) {
		go func() {
			ch <- true
		}()

		return pcs.pathChildren, pcs.pathStat, pcs.pathEvents, nil
	}

	pc, _, _ := NewPathCache(pcs.path, pcs.depth, pcs.serviceManager)
	defer pc.Stop()

	if assertChannelReceived(ch, 10*time.Millisecond) {
		t.Fatal("ChildrenW should not called when creating PathCache with depth is 0")
	}
}

func TestHasChildren(t *testing.T) {
	ch := make(chan bool)

	pcsParent := newTestPathCache("/parent", []byte(""), 1)
	defer pcsParent.cleanup()

	pcsParent.pathChildren = []string{"child"}

	pcsChild := newTestPathCache("/parent/child", []byte(""), 0)
	defer pcsChild.cleanup()

	pcsParent.zkConn.GetWFunc = func(path string) (b []byte, st *zk.Stat, ev <-chan zk.Event, err error) {
		if path == "/parent/child" {
			go func() {
				ch <- true
			}()

			return pcsChild.pathValue, pcsChild.pathStat, pcsChild.pathChildrenEvents, nil
		}

		return pcsParent.pathValue, pcsParent.pathStat, pcsParent.pathEvents, nil
	}

	pcsParent.zkConn.ChildrenWFunc = func(path string) (children []string, st *zk.Stat, ev <-chan zk.Event, err error) {
		if path == "/parent/child" {
			return pcsChild.pathChildren, pcsChild.pathStat, pcsChild.pathEvents, nil
		}

		return pcsParent.pathChildren, pcsParent.pathStat, pcsParent.pathEvents, nil
	}

	pc, events, err := NewPathCache(pcsParent.path, pcsParent.depth, pcsParent.serviceManager)
	defer pc.Stop()

	if err != nil {
		t.Error(err)
	}

	ev := <-events

	if ev.Type != PathCacheAddNotification || ev.Path != pcsChild.path {
		t.Fatal("Parent PathCache not notified of new child")
	}

	if !assertChannelReceived(ch, 10*time.Millisecond) {
		t.Fatal("GetW should be called for child")
	}
}

func TestDecreaseDepthWhenCreatingChild(t *testing.T) {
	pcsParent := newTestPathCache("/parent", []byte(""), 1)
	defer pcsParent.cleanup()

	pcsParent.pathChildren = []string{"child"}

	pcsChild := newTestPathCache("/parent/child", []byte(""), 0)
	defer pcsChild.cleanup()

	pcsParent.zkConn.GetWFunc = func(path string) (b []byte, st *zk.Stat, ev <-chan zk.Event, err error) {
		if path == "/parent/child" {
			return pcsChild.pathValue, pcsChild.pathStat, pcsChild.pathChildrenEvents, nil
		}

		return pcsParent.pathValue, pcsParent.pathStat, pcsParent.pathEvents, nil
	}

	pcsParent.zkConn.ChildrenWFunc = func(path string) (children []string, st *zk.Stat, ev <-chan zk.Event, err error) {
		if path == "/parent/child" {
			t.Fatal("ChildrenW should not be called on child when depth has been decreased")

			return pcsChild.pathChildren, pcsChild.pathStat, pcsChild.pathEvents, nil
		}

		return pcsParent.pathChildren, pcsParent.pathStat, pcsParent.pathEvents, nil
	}

	pc, events, err := NewPathCache(pcsParent.path, pcsParent.depth, pcsParent.serviceManager)
	defer pc.Stop()

	if err != nil {
		t.Error(err)
	}

	// Read child add
	<-events

	if pc.children["/parent/child"].depth != 0 {
		t.Fatal("Depth should be decreased for child")
	}
}

func TestAddChild(t *testing.T) {
	pcsParent := newTestPathCache("/parent", []byte(""), 1)
	defer pcsParent.cleanup()

	pcsParent.pathChildren = []string{"child"}

	pcsChild := newTestPathCache("/parent/child", []byte(""), 0)
	defer pcsChild.cleanup()

	pcsChild2 := newTestPathCache("/parent/child2", []byte(""), 0)
	defer pcsChild2.cleanup()

	pcsParent.zkConn.GetWFunc = func(path string) (b []byte, st *zk.Stat, ev <-chan zk.Event, err error) {
		if path == "/parent/child" {
			return pcsChild.pathValue, pcsChild.pathStat, pcsChild.pathEvents, nil
		} else if path == "/parent/child2" {
			return pcsChild2.pathValue, pcsChild2.pathStat, pcsChild2.pathEvents, nil
		}

		return pcsParent.pathValue, pcsParent.pathStat, pcsParent.pathEvents, nil
	}

	pcsParent.zkConn.ChildrenWFunc = func(path string) (children []string, st *zk.Stat, ev <-chan zk.Event, err error) {
		if path == "/parent/child" {
			return pcsChild.pathChildren, pcsChild.pathStat, pcsChild.pathChildrenEvents, nil
		} else if path == "/parent/child2" {
			return pcsChild2.pathChildren, pcsChild2.pathStat, pcsChild2.pathChildrenEvents, nil
		}

		return pcsParent.pathChildren, pcsParent.pathStat, pcsParent.pathChildrenEvents, nil
	}

	pc, events, err := NewPathCache(pcsParent.path, pcsParent.depth, pcsParent.serviceManager)
	defer pc.Stop()

	if err != nil {
		t.Error(err)
	}

	// Read initial child add
	<-events

	// Add an additional child
	pcsParent.pathChildren = append(pcsParent.pathChildren, "child2")
	go pcsParent.sendChildEvent(zk.EventNodeChildrenChanged, pcsParent.path)

	ev := <-events

	if ev.Type != PathCacheAddNotification || ev.Path != pcsChild2.path {
		t.Fatal("Parent PathCache not notified of new child")
	}
}

func TestRemoveChild(t *testing.T) {
	pcsParent := newTestPathCache("/parent", []byte(""), 1)
	defer pcsParent.cleanup()

	pcsParent.pathChildren = []string{"child"}

	pcsChild := newTestPathCache("/parent/child", []byte(""), 0)
	defer pcsChild.cleanup()

	pcsParent.zkConn.GetWFunc = func(path string) (b []byte, st *zk.Stat, ev <-chan zk.Event, err error) {
		if path == "/parent/child" {
			return pcsChild.pathValue, pcsChild.pathStat, pcsChild.pathEvents, nil
		}

		return pcsParent.pathValue, pcsParent.pathStat, pcsParent.pathEvents, nil
	}

	pcsParent.zkConn.ChildrenWFunc = func(path string) (children []string, st *zk.Stat, ev <-chan zk.Event, err error) {
		if path == "/parent/child" {
			return pcsChild.pathChildren, pcsChild.pathStat, pcsChild.pathChildrenEvents, nil
		}

		return pcsParent.pathChildren, pcsParent.pathStat, pcsParent.pathChildrenEvents, nil
	}

	pc, events, err := NewPathCache(pcsParent.path, pcsParent.depth, pcsParent.serviceManager)
	defer pc.Stop()

	if err != nil {
		t.Error(err)
	}

	// Read initial child add
	<-events

	// Remove the child
	pcsParent.pathChildren = pcsParent.pathChildren[1:]
	go pcsChild.sendEvent(zk.EventNodeDeleted, pcsChild.path)

	ev := <-events

	if ev.Type != PathCacheRemoveNotification || ev.Path != pcsChild.path {
		t.Fatal("Parent PathCache not notified of removed child")
	}

	if len(pc.children) > 0 {
		t.Fatal("child not removed")
	}
}

func TestPathValue(t *testing.T) {
	ch := make(chan bool)

	pcsParent := newTestPathCache("/parent", []byte(""), 1)
	defer pcsParent.cleanup()

	pcsParent.pathChildren = []string{"child"}

	pcsChild := newTestPathCache("/parent/child", []byte(""), 0)
	defer pcsChild.cleanup()

	pcsParent.zkConn.GetWFunc = func(path string) (b []byte, st *zk.Stat, ev <-chan zk.Event, err error) {
		if path == "/parent/child" {
			go func() {
				ch <- true
			}()

			return pcsChild.pathValue, pcsChild.pathStat, pcsChild.pathChildrenEvents, nil
		}

		return pcsParent.pathValue, pcsParent.pathStat, pcsParent.pathEvents, nil
	}

	pcsParent.zkConn.ChildrenWFunc = func(path string) (children []string, st *zk.Stat, ev <-chan zk.Event, err error) {
		if path == "/parent/child" {
			return pcsChild.pathChildren, pcsChild.pathStat, pcsChild.pathEvents, nil
		}

		return pcsParent.pathChildren, pcsParent.pathStat, pcsParent.pathEvents, nil
	}

	pc, events, err := NewPathCache(pcsParent.path, pcsParent.depth, pcsParent.serviceManager)
	defer pc.Stop()

	if err != nil {
		t.Error(err)
	}

	waitForNotification(events, PathCacheAddNotification, pcsChild.path)

	val := pc.PathValue("/parent/child")

	if !bytes.Equal(val, pcsParent.pathValue) {
		t.Fatal("Retrieved invalid PathValue", string(val), string(pcsChild.pathValue))
	}
}

// Ensure one event doesn't cause adverse effects that prevent additional ones from being picked up
func TestMultipleEventsOnPathAndChildren(t *testing.T) {
	pcsParent := newTestPathCache("/parent", []byte(""), 1)
	defer pcsParent.cleanup()

	pcsParent.pathChildren = []string{"child"}

	pcsChild := newTestPathCache("/parent/child", []byte(""), 0)
	defer pcsChild.cleanup()

	pcsChild2 := newTestPathCache("/parent/child2", []byte(""), 0)
	defer pcsChild2.cleanup()

	pcsParent.zkConn.GetWFunc = func(path string) (b []byte, st *zk.Stat, ev <-chan zk.Event, err error) {
		if path == "/parent/child" {
			return pcsChild.pathValue, pcsChild.pathStat, pcsChild.pathEvents, nil
		} else if path == "/parent/child2" {
			return pcsChild2.pathValue, pcsChild2.pathStat, pcsChild2.pathEvents, nil
		}

		return pcsParent.pathValue, pcsParent.pathStat, pcsParent.pathEvents, nil
	}

	pcsParent.zkConn.ChildrenWFunc = func(path string) (children []string, st *zk.Stat, ev <-chan zk.Event, err error) {
		if path == "/parent/child" {
			return pcsChild.pathChildren, pcsChild.pathStat, pcsChild.pathChildrenEvents, nil
		} else if path == "/parent/child2" {
			return pcsChild2.pathChildren, pcsChild2.pathStat, pcsChild2.pathChildrenEvents, nil
		}

		return pcsParent.pathChildren, pcsParent.pathStat, pcsParent.pathChildrenEvents, nil
	}

	pc, events, err := NewPathCache(pcsParent.path, pcsParent.depth, pcsParent.serviceManager)
	defer pc.Stop()

	if err != nil {
		t.Error(err)
	}

	// Read initial child add
	<-events

	// Add an additional child
	pcsParent.pathChildren = append(pcsParent.pathChildren, "child2")
	go pcsParent.sendChildEvent(zk.EventNodeChildrenChanged, pcsParent.path)

	ev := <-events

	if ev.Type != PathCacheAddNotification || ev.Path != pcsChild2.path {
		t.Fatal("Parent PathCache not notified of new child")
	}

	// Update parent
	pcsParent.updateValue([]byte("Update1"))
	waitForNotification(events, PathCacheUpdateNotification, pcsParent.path)

	val := pc.Value()

	if !bytes.Equal(val, pcsParent.pathValue) {
		t.Fatal("PathCache did not update value:", string(val), string(pcsParent.pathValue))
	}

	// Update Child
	pcsChild.updateValue([]byte("ChildUpdate1"))
	waitForNotification(events, PathCacheUpdateNotification, pcsChild.path)

	val = pc.Value()

	if !bytes.Equal(val, pcsParent.pathValue) {
		t.Fatal("PathCache did not update value:", string(val), string(pcsParent.pathValue))
	}

	// Remove Child
	pcsParent.pathChildren = pcsParent.pathChildren[1:]
	go pcsChild.sendEvent(zk.EventNodeDeleted, pcsChild.path)
	waitForNotification(events, PathCacheRemoveNotification, pcsChild.path)

	if len(pc.children) != 1 {
		t.Fatal("Child not removed")
	}

	// Add child back
	pcsParent.pathChildren = append(pcsParent.pathChildren, "child")
	go pcsParent.sendChildEvent(zk.EventNodeChildrenChanged, pcsParent.path)
	waitForNotification(events, PathCacheAddNotification, pcsChild.path)

	// Update child again
	pcsChild.updateValue([]byte("ChildUpdate2"))
	waitForNotification(events, PathCacheUpdateNotification, pcsChild.path)

	val = pc.Value()

	if !bytes.Equal(val, pcsParent.pathValue) {
		t.Fatal("PathCache did not update value:", string(val), string(pcsParent.pathValue))
	}

	// Update parent again
	pcsParent.updateValue([]byte("Update2"))
	waitForNotification(events, PathCacheUpdateNotification, pcsParent.path)

	val = pc.Value()

	if !bytes.Equal(val, pcsParent.pathValue) {
		t.Fatal("PathCache did not update value:", string(val), string(pcsParent.pathValue))
	}

	// Remove child again
	pcsParent.pathChildren = pcsParent.pathChildren[1:]
	go pcsChild.sendEvent(zk.EventNodeDeleted, pcsChild.path)
	waitForNotification(events, PathCacheRemoveNotification, pcsChild.path)

	if len(pc.children) != 1 {
		t.Fatal("Child not removed")
	}

	// Remove remaining child
	pcsParent.pathChildren = pcsParent.pathChildren[1:]
	go pcsChild2.sendEvent(zk.EventNodeDeleted, pcsChild2.path)
	waitForNotification(events, PathCacheRemoveNotification, pcsChild2.path)

	if len(pc.children) != 0 {
		t.Fatal("Child not removed")
	}
}

type pathCacheStub struct {
	session chan zk.Event

	zkConn         *TestConnection
	serviceManager *ZookeeperServiceManager

	depth int

	path               string
	pathEvents         chan zk.Event
	pathValue          []byte
	pathStat           *zk.Stat
	pathChildren       []string
	pathChildrenEvents chan zk.Event
}

func (pcs *pathCacheStub) PathCache() (*PathCache, chan PathCacheNotification, error) {
	return NewPathCache(pcs.path, pcs.depth, pcs.serviceManager)
}

func waitForNotification(ch chan PathCacheNotification, typ PathCacheNotificationType, path string) {
	timeout := time.After(100 * time.Millisecond)

	for {
		select {
		case n := <-ch:
			if n.Type == typ && n.Path == path {
				return
			}
		case <-timeout:
			panic("timed out waiting for notification")
		}
	}
}

func (pcs *pathCacheStub) updateValue(v []byte) {
	pcs.pathValue = []byte("Update1")
	pcs.sendEvent(zk.EventNodeDataChanged, pcs.path)
}

func (pcs *pathCacheStub) sendEvent(typ zk.EventType, path string) {
	pcs.pathEvents <- zk.Event{Type: typ, Path: path}
}

func (pcs *pathCacheStub) sendChildEvent(typ zk.EventType, path string) {
	pcs.pathChildrenEvents <- zk.Event{Type: typ, Path: path}
}

func (pcs *pathCacheStub) cleanup() {
}

func newTestPathCache(path string, val []byte, depth int) *pathCacheStub {
	pcs := &pathCacheStub{
		session: make(chan zk.Event),

		depth: depth,

		path:               path,
		pathEvents:         make(chan zk.Event),
		pathValue:          val,
		pathStat:           &zk.Stat{},
		pathChildren:       make([]string, 0),
		pathChildrenEvents: make(chan zk.Event),
	}

	pcs.zkConn = &TestConnection{
		GetWFunc: func(path string) (b []byte, st *zk.Stat, ev <-chan zk.Event, err error) {
			return pcs.pathValue, pcs.pathStat, pcs.pathEvents, nil
		},
		ChildrenWFunc: func(path string) (children []string, st *zk.Stat, ev <-chan zk.Event, err error) {
			return pcs.pathChildren, pcs.pathStat, pcs.pathChildrenEvents, nil
		},
	}

	factory = func(servers []string, recvTimeout time.Duration) (ZkConnection, <-chan zk.Event, error) {
		return pcs.zkConn, pcs.session, nil
	}

	pcs.serviceManager = &ZookeeperServiceManager{
		conn: pcs.zkConn,
	}

	return pcs
}

func assertChannelReceived(ch chan bool, d time.Duration) bool {
	timeout := time.After(d)
	resp := make(chan bool)

	go func() {
		select {
		case <-ch:
			resp <- true
		case <-timeout:
			resp <- false
		}
	}()

	return <-resp
}
