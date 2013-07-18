package zkmanager

import (
	"fmt"
	"github.com/petar/gozk"
	"github.com/skynetservices/skynet2"
	"github.com/skynetservices/skynet2/log"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

/*
  TODO: Lot's of testing and error handling
        The implementation for rebuilding the cache on a set interval needs to be changed,
        this is a temporary bandaid to be able to launch some business priority projects that have a set
        release schedule, it will be replaced by proper watches for incremental updates of the cache based
        on changes observed in zookeeper.
*/
type ZookeeperServiceManager struct {
	conn            *zookeeper.Conn
	servers         string
	timeout         time.Duration
	clientId        *zookeeper.ClientId
	registeredCache map[string][]string
	serviceCache    map[string][]string
	regionCache     map[string][]string
	hostCache       map[string][]string
	instanceCache   map[string]skynet.ServiceInfo
	cacheMutex      sync.Mutex
	done            chan bool
	session         <-chan zookeeper.Event
	tick            <-chan time.Time
}

func NewZookeeperServiceManager(servers string, timeout time.Duration) skynet.ServiceManager {
	zk, session, err := zookeeper.Dial(servers, timeout)
	if err != nil {
		log.Panic("Couldn't connect: " + err.Error())
	}

	// Wait for connection.
	event := <-session

	if event.State != zookeeper.STATE_CONNECTED {
		log.Panic("Couldn't connect to zookeeper")
	}

	sm := &ZookeeperServiceManager{
		conn:            zk,
		servers:         servers,
		timeout:         timeout,
		clientId:        zk.ClientId(),
		instanceCache:   make(map[string]skynet.ServiceInfo),
		registeredCache: make(map[string][]string),
		serviceCache:    make(map[string][]string),
		regionCache:     make(map[string][]string),
		hostCache:       make(map[string][]string),
		done:            make(chan bool),
		session:         session,
		tick:            time.Tick(15 * time.Second),
	}

	sm.createDefaultPaths()
	sm.buildCache()
	go sm.mux()

	return sm
}

func (sm *ZookeeperServiceManager) Shutdown() (err error) {
	sm.done <- true
	sm.conn.Close()

	return
}

func (sm *ZookeeperServiceManager) Add(s skynet.ServiceInfo) (err error) {
	log.Println(log.TRACE, "Adding service to cluster", s.UUID)

	// Create path to store instance data
	err = sm.createPath(path.Join("/instances", s.UUID))
	if err != nil {
		return
	}

	for k, v := range getValuesForService(s) {
		_, err = sm.conn.Create(path.Join("/instances", s.UUID, k), v, zookeeper.EPHEMERAL, zookeeper.WorldACL(zookeeper.PERM_ALL))
		if err != nil {
			return
		}
	}

	sm.announceInstance(getPathsForInstance(s), s.UUID)
	return
}

func (sm *ZookeeperServiceManager) Update(s skynet.ServiceInfo) (err error) {
	log.Println(log.TRACE, "Updating service", s.UUID)

	for k, v := range getValuesForService(s) {
		_, err = sm.conn.Set(path.Join("/instances", s.UUID, k), v, -1)
		if err != nil {
			return
		}
	}

	return
}

func (sm *ZookeeperServiceManager) Remove(s skynet.ServiceInfo) (err error) {
	log.Println(log.TRACE, "Removing service", s.UUID)

	err = sm.removeInstance(getPathsForInstance(s), s.UUID)
	if err != nil {
		return
	}

	err = sm.deleteRecursive(path.Join("/instances", s.UUID))
	return
}

func (sm *ZookeeperServiceManager) Register(uuid string) (err error) {
	log.Println(log.TRACE, "Registering service", uuid)

	_, err = sm.conn.Set(path.Join("/instances", uuid, "registered"), "false", -1)
	return
}

func (sm *ZookeeperServiceManager) Unregister(uuid string) (err error) {
	log.Println(log.TRACE, "Unregister service", uuid)

	_, err = sm.conn.Set(path.Join("/instances", uuid, "registered"), "false", -1)
	return
}

// Retrieve ServiceInfo from zookeeper
func (sm *ZookeeperServiceManager) getServiceInfo(uuid string) (s skynet.ServiceInfo, err error) {
	s.ServiceConfig = new(skynet.ServiceConfig)
	s.UUID = uuid

	reg, _, err := sm.conn.Get(path.Join("/instances", uuid, "registered"))

	if err != nil {
		return
	}

	if reg == "true" {
		s.Registered = true
	} else {
		s.Registered = false
	}

	addr, _, err := sm.conn.Get(path.Join("/instances", uuid, "addr"))

	if err != nil {
		return
	}

	s.ServiceAddr, err = skynet.BindAddrFromString(addr)

	if err != nil {
		return
	}

	s.Name, _, err = sm.conn.Get(path.Join("/instances", uuid, "name"))

	if err != nil {
		return
	}

	s.Version, _, err = sm.conn.Get(path.Join("/instances", uuid, "version"))

	if err != nil {
		return
	}

	s.Region, _, err = sm.conn.Get(path.Join("/instances", uuid, "region"))

	if err != nil {
		return
	}

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

// Creates all paths as Permanent, and adds a node for the UUID which is Ephemeral
func (sm *ZookeeperServiceManager) announceInstance(paths []string, uuid string) (err error) {
	for _, p := range paths {
		err = sm.createPath(p)
		if err != nil {
			return
		}

		_, err = sm.conn.Create(path.Join(p, uuid), "", zookeeper.EPHEMERAL, zookeeper.WorldACL(zookeeper.PERM_ALL))
		if err != nil {
			fmt.Println(err.Error())
			return
		}
	}

	return
}

func (sm *ZookeeperServiceManager) removeInstance(paths []string, uuid string) (err error) {
	for _, p := range paths {
		err = sm.conn.Delete(path.Join(p, uuid), -1)
		if err != nil {
			return
		}
	}

	return
}

// Creates default paths we'll be registering with
func (sm *ZookeeperServiceManager) createDefaultPaths() (err error) {
	return sm.createPaths([]string{
		"/hosts",
		"/instances",
		"/regions",
		"/services",
	})
}

// Calls createPath on all paths contained in the slice
func (sm *ZookeeperServiceManager) createPaths(paths []string) (err error) {
	for _, p := range paths {
		err = sm.createPath(p)
		if err != nil {
			return
		}
	}

	return
}

// Creates path recursively, shortcut to manually creating each path section
// paths are created as permanent, must manually create paths that are needed to be Ephemeral
func (sm *ZookeeperServiceManager) createPath(path string) error {
	parts := strings.Split(path, "/")
	path = ""

	for _, p := range parts {
		if p == "" {
			continue
		}

		path = path + "/" + p

		if stat, _ := sm.conn.Exists(path); stat != nil {
			continue
		}

		_, err := sm.conn.Create(path, "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))

		if err != nil {
			log.Println(log.ERROR, err.Error())
			return err
		}
	}

	return nil
}

// Delete path and all children
func (sm *ZookeeperServiceManager) deleteRecursive(p string) (err error) {
	children, _, err := sm.conn.Children(p)

	if err != nil {
		return
	}

	// Delete all children first
	if len(children) > 0 {
		for _, c := range children {
			sm.deleteRecursive(path.Join(p, c))
		}
	}

	// Delete path
	err = sm.conn.Delete(p, -1)

	return
}

func (sm *ZookeeperServiceManager) buildCache() error {
	instanceCache := make(map[string]skynet.ServiceInfo)
	registeredCache := make(map[string][]string)
	serviceCache := make(map[string][]string)
	regionCache := make(map[string][]string)
	hostCache := make(map[string][]string)

	uuids, _, err := sm.conn.Children("/instances")
	if err != nil {
		log.Println(log.ERROR, "Failed to get instances for cache", err)
		return err
	}

	for _, uuid := range uuids {
		instance, e := sm.getServiceInfo(uuid)

		if e != nil {
			// This is probably stale data that needs to be cleaned up
			continue
		}

		instanceCache[instance.UUID] = instance

		sm.addToRegionCache(instance, &regionCache)
		sm.addToRegisteredCache(instance, &registeredCache)
		sm.addToServiceCache(instance, &serviceCache)
		sm.addToHostCache(instance, &hostCache)
	}

	sm.cacheMutex.Lock()
	defer sm.cacheMutex.Unlock()

	sm.instanceCache = instanceCache
	sm.registeredCache = registeredCache
	sm.serviceCache = serviceCache
	sm.regionCache = regionCache
	sm.hostCache = hostCache

	return nil
}

func (sm *ZookeeperServiceManager) addToRegionCache(instance skynet.ServiceInfo, regionCache *map[string][]string) {
	if _, ok := (*regionCache)[instance.Region]; !ok {
		(*regionCache)[instance.Region] = make([]string, 0, 10)
	}

	(*regionCache)[instance.Region] = append((*regionCache)[instance.Region], instance.UUID)
}

func (sm *ZookeeperServiceManager) addToRegisteredCache(instance skynet.ServiceInfo, registeredCache *map[string][]string) {
	registered := strconv.FormatBool(instance.Registered)

	if _, ok := sm.registeredCache[registered]; !ok {
		(*registeredCache)[registered] = make([]string, 0, 10)
	}

	(*registeredCache)[registered] = append((*registeredCache)[registered], instance.UUID)
}

func (sm *ZookeeperServiceManager) addToServiceCache(instance skynet.ServiceInfo, serviceCache *map[string][]string) {
	// Add for just service name
	service := instance.Name

	if _, ok := (*serviceCache)[service]; !ok {
		(*serviceCache)[service] = make([]string, 0, 10)
	}

	(*serviceCache)[service] = append((*serviceCache)[service], instance.UUID)

	// Add name and version
	service = instance.Name + "::" + instance.Version

	if _, ok := sm.serviceCache[service]; !ok {
		(*serviceCache)[service] = make([]string, 0, 10)
	}

	(*serviceCache)[service] = append((*serviceCache)[service], instance.UUID)
}

func (sm *ZookeeperServiceManager) addToHostCache(instance skynet.ServiceInfo, hostCache *map[string][]string) {
	host := instance.ServiceAddr.IPAddress

	if _, ok := (*hostCache)[host]; !ok {
		(*hostCache)[host] = make([]string, 0, 10)
	}

	(*hostCache)[host] = append((*hostCache)[host], instance.UUID)
}

func (sm *ZookeeperServiceManager) mux() {
	for {
		select {
		case <-sm.done:
			return
		case event := <-sm.session:
			log.Println(log.TRACE, "session event received:", event.String())
		case <-sm.tick:
			sm.buildCache()
		}
	}
}
