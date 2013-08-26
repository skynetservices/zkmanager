package zkmanager

import (
	"github.com/samuel/go-zookeeper/zk"
	"github.com/skynetservices/skynet2/log"
	"path"
	"strings"
	"sync"
	"time"
)

type PathCacheNotificationType int

func (n PathCacheNotificationType) String() (s string) {
	switch n {
	case PathCacheAddNotification:
		s = "PathCacheAddNotification"
	case PathCacheUpdateNotification:
		s = "PathCacheUpdateNotification"
	case PathCacheRemoveNotification:
		s = "PathCacheRemoveNotification"
	}

	return
}

const (
	_ PathCacheNotificationType = iota
	PathCacheAddNotification
	PathCacheUpdateNotification
	PathCacheRemoveNotification
)

type PathCacheNotification struct {
	Type  PathCacheNotificationType
	Path  string
	Depth int
}

type PathCache struct {
	path           string
	stat           *zk.Stat
	value          []byte
	depth          int
	children       map[string]*PathCache
	serviceManager *ZookeeperServiceManager

	events       chan zk.Event
	setValueChan chan pathValue
	getValueChan chan chan []byte

	addChildChan    chan string
	childNotifyChan chan PathCacheNotification

	notifyChan  chan PathCacheNotification
	stopChan    chan bool
	stoppedChan chan bool

	startup sync.WaitGroup
}

type pathValue struct {
	Data []byte
	Stat *zk.Stat
}

func NewPathCache(path string, depth int, sm *ZookeeperServiceManager) (pc *PathCache, notifyChan chan PathCacheNotification, err error) {
	pc = &PathCache{
		path:           path,
		depth:          depth,
		children:       make(map[string]*PathCache),
		serviceManager: sm,
		events:         make(chan zk.Event, 10),

		setValueChan: make(chan pathValue),
		getValueChan: make(chan chan []byte),

		addChildChan:    make(chan string, 10),
		childNotifyChan: make(chan PathCacheNotification, 10),

		notifyChan: make(chan PathCacheNotification, 10),

		stopChan:    make(chan bool, 1),
		stoppedChan: make(chan bool, 1),
	}

	_, err = pc.Start()

	return pc, pc.notifyChan, err
}

func (pc *PathCache) Start() (notifyChan chan PathCacheNotification, err error) {
	pc.notifyChan = make(chan PathCacheNotification, 10)
	pc.stoppedChan = make(chan bool, 1)
	pc.startup = sync.WaitGroup{}

	go pc.mux()

	err = pc.watch()

	if err != nil {
		// We expect this to occasionally happen due to timing
		if err != zk.ErrNoNode {
			log.Println(log.ERROR, err)
		}

		pc.Stop()
		return
	}

	err = pc.watchChildren()

	if err != nil {
		// We expect this to occasionally happen due to timing
		if err != zk.ErrNoNode {
			log.Println(log.ERROR, err)
		}
		pc.Stop()
	}

	pc.startup.Wait()

	return pc.notifyChan, err
}

func (pc *PathCache) Value() []byte {
	c := make(chan []byte)
	pc.getValueChan <- c

	v := <-c

	return v
}

func (pc *PathCache) PathValue(p string) (b []byte) {
	if p == pc.path {
		return pc.Value()
	}

	if strings.HasPrefix(p, pc.path) {
		parts := strings.Split(strings.TrimPrefix(p, pc.path), "/")
		childKey := path.Join(pc.path, parts[1])

		if child, ok := pc.children[childKey]; ok {
			return child.PathValue(p)
		}
	}

	return
}

func (pc *PathCache) NumChildren() int {
	return len(pc.children)
}

func (pc *PathCache) Children() (children []string) {
	for c, _ := range pc.children {
		children = append(children, c)
	}

	return
}

func (pc *PathCache) Restart() (notifyChan chan PathCacheNotification, err error) {
	pc.Stop()

	// wait for stop to complete
	<-pc.stoppedChan

	return pc.Start()
}

func (pc *PathCache) Stop() {
	go func() {
		pc.stopChan <- true
	}()
}

func (pc *PathCache) watch() error {
	pc.startup.Add(1)

	value, stat, ev, err := pc.serviceManager.conn.GetW(pc.path)

	if err != nil {
		// We expect this to occasionally happen due to timing
		if err != zk.ErrNoNode {
			log.Println(log.ERROR, err)
		}
		return err
	}

	go forwardZkEvents(ev, pc.events)

	pc.setValueChan <- pathValue{Data: value, Stat: stat}

	pc.startup.Done()

	return nil
}

func (pc *PathCache) watchChildren() error {
	if pc.depth == 0 {
		return nil
	}

	children, _, ev, err := pc.serviceManager.conn.ChildrenW(pc.path)

	if err != nil {
		if err != zk.ErrNoNode {
			log.Println(log.ERROR, err)
		}
		return err
	}

	go forwardZkEvents(ev, pc.events)

	pc.startup.Add(len(children))
	for _, c := range children {
		if _, ok := pc.children[path.Join(pc.path, c)]; !ok {
			go func(c string) {
				pc.addChildChan <- c
			}(c)
		}
	}

	return nil
}

func (pc *PathCache) mux() {
	for {
		select {
		case pv := <-pc.setValueChan:
			pc.value = pv.Data
			pc.stat = pv.Stat

		case c := <-pc.getValueChan:
			c <- pc.value
		case p := <-pc.addChildChan:
			depth := pc.depth
			path := path.Join(pc.path, p)

			if depth > 0 {
				depth = depth - 1
			}

			pchild, nChan, err := NewPathCache(path, depth, pc.serviceManager)
			if err != nil {
				log.Println(log.ERROR, "Error creating child path", err.Error())
				pc.startup.Done()
				continue
			}

			pc.children[path] = pchild
			pc.notifyParent(PathCacheNotification{Type: PathCacheAddNotification, Path: path})

			go forwardNotifications(nChan, pc.childNotifyChan, 1*time.Second)

			pc.startup.Done()
		case n := <-pc.childNotifyChan:
			// We only know about our direct children, skip anything with a depth that is not 1 (directly below us)
			if n.Type == PathCacheRemoveNotification && n.Depth == 1 {
				delete(pc.children, n.Path)
			}

			// We dont do anything personally with adds and updates as we
			// can crawl the hierarchy ourselves, parents may want to know
			// increment Depth for clarity of how many levels below this was generated
			pc.notifyParent(n)
		case ev := <-pc.events:
			switch ev.Type {
			case zk.EventNodeDataChanged:
				pc.notifyParent(PathCacheNotification{Type: PathCacheUpdateNotification, Path: ev.Path})
				go pc.watch()

			case zk.EventNodeDeleted:
				pc.notifyParent(PathCacheNotification{Type: PathCacheRemoveNotification, Path: ev.Path})

				if ev.Path == pc.path {
					go pc.Stop()
				}

				return
			case zk.EventNodeChildrenChanged:
				// Children catch their own deletions and propogate the notification
				// watchChildren will check for missing paths
				go pc.watchChildren()

			}
		case _, ok := <-pc.stopChan:
			if ok {
				for p, c := range pc.children {
					c.Stop()
					delete(pc.children, p)
				}

				close(pc.notifyChan)

				pc.stoppedChan <- true

				close(pc.stoppedChan)
			}
			return
		}
	}
}

func forwardZkEvents(src <-chan zk.Event, dst chan zk.Event) {
	event, ok := <-src

	if ok {
		dst <- event
	}
}

func forwardNotifications(src, dst chan PathCacheNotification, timeout time.Duration) {
	for {
		select {
		case n, ok := <-src:
			if !ok {
				return
			}

			go sendNotificationTimeout(dst, n, timeout)
		}
	}
}

func sendNotificationTimeout(dst chan PathCacheNotification, n PathCacheNotification, timeout time.Duration) {
	if timeout == 0 {
		dst <- n
		return
	}

	t := time.After(1 * time.Second)

	select {
	case dst <- n:
		// There is a possibility the  notify channel is not being received on
		// to minimize the number of these goroutines that pile up let's timeout
	case <-t:
		return
	}
}

func (pc *PathCache) notifyParent(n PathCacheNotification) {
	n.Depth++

	go sendNotificationTimeout(pc.notifyChan, n, 1*time.Second)
}
