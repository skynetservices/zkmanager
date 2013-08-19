package zkmanager

import (
	"github.com/samuel/go-zookeeper/zk"
	"github.com/skynetservices/skynet2"
	"github.com/skynetservices/skynet2/log"
	"path"
	"strconv"
	"strings"
	"time"
)

type watcher struct {
	criteria skynet.CriteriaMatcher
	ch       <-chan skynet.InstanceNotification
}

type zkConnector func(servers []string, recvTimeout time.Duration) (ZkConnection, <-chan zk.Event, error)

var factory zkConnector = defaultFactory

type ZookeeperServiceManager struct {
	conn          ZkConnection
	done          chan bool
	session       <-chan zk.Event
	watchers      []watcher
	serviceCache  map[string][]string
	regionCache   map[string][]string
	hostCache     map[string][]string
	instanceCache map[string]skynet.ServiceInfo
}

func NewZookeeperServiceManager(servers string, timeout time.Duration) skynet.ServiceManager {
	sm := &ZookeeperServiceManager{
		done:          make(chan bool),
		watchers:      make([]watcher, 0, 0),
		instanceCache: make(map[string]skynet.ServiceInfo),
		serviceCache:  make(map[string][]string),
		regionCache:   make(map[string][]string),
		hostCache:     make(map[string][]string),
	}

	c, session, err := factory(strings.Split(servers, ","), timeout)

	if err != nil {
		log.Panic(err)
	}

	sm.conn = c
	sm.session = session

	go sm.mux()

	return sm
}

func (sm *ZookeeperServiceManager) Shutdown() (err error) {
	sm.done <- true

	return
}

func (sm *ZookeeperServiceManager) Add(s skynet.ServiceInfo) (err error) {
	log.Println(log.TRACE, "Adding service to cluster", s.UUID)

	return
}

func (sm *ZookeeperServiceManager) Update(s skynet.ServiceInfo) (err error) {
	log.Println(log.TRACE, "Updating service", s.UUID)

	return
}

func (sm *ZookeeperServiceManager) Remove(s skynet.ServiceInfo) (err error) {
	log.Println(log.TRACE, "Removing service", s.UUID)
	return
}

func (sm *ZookeeperServiceManager) Register(uuid string) (err error) {
	log.Println(log.TRACE, "Registering service", uuid)

	return
}

func (sm *ZookeeperServiceManager) Unregister(uuid string) (err error) {
	log.Println(log.TRACE, "Unregister service", uuid)

	return
}

func (sm *ZookeeperServiceManager) Watch(criteria skynet.CriteriaMatcher, c <-chan skynet.InstanceNotification) (instances []skynet.ServiceInfo) {
	sm.watchers = append(sm.watchers, watcher{criteria, c})

	// TODO: return current list of matching instances
	return
}

// Retrieve ServiceInfo from zookeeper
func (sm *ZookeeperServiceManager) getServiceInfo(uuid string) (s skynet.ServiceInfo, err error) {
	return
}

// Converts ServiceInfo to a map to be used for setting values
func getValuesForService(s skynet.ServiceInfo) (values map[string]string) {
	return map[string]string{
		"registered": strconv.FormatBool(s.Registered),
		"addr":       s.ServiceAddr.String(),
		"name":       s.Name,
		"version":    s.Version,
		"region":     s.Region,
	}
}

// Returns a list of paths for a uuid to be dropped to broadcast this service
func getPathsForInstance(s skynet.ServiceInfo) []string {
	return []string{
		path.Join("/regions", s.Region),
		path.Join("/services", s.Name),
		path.Join("/services", s.Name, s.Version),
		path.Join("/hosts", s.ServiceAddr.IPAddress),
	}
}

func (sm *ZookeeperServiceManager) mux() {
	// TODO: EventNotWatching
	// TODO: StateDisconnected
	for {
		select {
		case e := <-sm.session:
			log.Println(log.TRACE, "Zookeeper Event Received: ", e)
		case <-sm.done:
			sm.conn.Close()
			return
		}
	}
}

func defaultFactory(servers []string, recvTimeout time.Duration) (ZkConnection, <-chan zk.Event, error) {
	return zk.Connect(servers, recvTimeout)
}
