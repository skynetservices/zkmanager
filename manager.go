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
	ch       chan<- skynet.InstanceNotification
}

type zkConnector func(servers []string, recvTimeout time.Duration) (ZkConnection, <-chan zk.Event, error)

var factory zkConnector = defaultFactory

type ZookeeperServiceManager struct {
	conn ZkConnection

	done    chan bool
	session <-chan zk.Event

	watchers []watcher

	cache *InstanceCache

	managedInstances map[string]skynet.ServiceInfo
}

func NewZookeeperServiceManager(servers string, timeout time.Duration) skynet.ServiceManager {
	sm := &ZookeeperServiceManager{
		done:             make(chan bool),
		watchers:         make([]watcher, 0, 0),
		managedInstances: make(map[string]skynet.ServiceInfo),
	}

	c, session, err := factory(strings.Split(servers, ","), timeout)

	if err != nil {
		log.Panic(err)
	}

	sm.conn = c
	sm.session = session

	err = sm.createDefaultPaths()

	if err != nil && err != zk.ErrNodeExists {
		log.Panic(err)
	}

	sm.cache, err = NewInstanceCache(sm)

	if err != nil {
		log.Panic(err)
	}

	go sm.mux()

	return sm
}

func (sm *ZookeeperServiceManager) Shutdown() (err error) {
	sm.done <- true

	return
}

func (sm *ZookeeperServiceManager) Add(s skynet.ServiceInfo) (err error) {
	log.Println(log.TRACE, "Adding service to cluster", s.UUID)

	err = sm.createPathsForService(s)

	if err != nil {
		return err
	}

	ops := zk.MultiOps{
		Create: []zk.CreateRequest{
			createRequest(path.Join("/regions", s.Region, s.UUID), []byte{}, zk.PermAll, zk.FlagEphemeral),
			createRequest(path.Join("/services", s.Name, s.UUID), []byte{}, zk.PermAll, zk.FlagEphemeral),
			createRequest(path.Join("/services", s.Name, s.Version, s.UUID), []byte{}, zk.PermAll, zk.FlagEphemeral),
			createRequest(path.Join("/hosts", s.ServiceAddr.IPAddress, s.UUID), []byte{}, zk.PermAll, zk.FlagEphemeral),
			createRequest(path.Join("/instances", s.UUID), []byte{}, zk.PermAll, 0),

			createRequest(path.Join("/instances", s.UUID, "registered"), []byte(strconv.FormatBool(s.Registered)), zk.PermAll, zk.FlagEphemeral),
			createRequest(path.Join("/instances", s.UUID, "name"), []byte(s.Name), zk.PermAll, zk.FlagEphemeral),
			createRequest(path.Join("/instances", s.UUID, "version"), []byte(s.Version), zk.PermAll, zk.FlagEphemeral),
			createRequest(path.Join("/instances", s.UUID, "region"), []byte(s.Region), zk.PermAll, zk.FlagEphemeral),
			createRequest(path.Join("/instances", s.UUID, "addr"), []byte(s.ServiceAddr.String()), zk.PermAll, zk.FlagEphemeral),
		},
	}

	err = sm.conn.Multi(ops)

	sm.managedInstances[s.UUID] = s

	return
}

func (sm *ZookeeperServiceManager) Update(s skynet.ServiceInfo) (err error) {
	log.Println(log.TRACE, "Updating service", s.UUID)

	ops := zk.MultiOps{
		SetData: []zk.SetDataRequest{
			setDataRequest(path.Join("/instances", s.UUID, "registered"), []byte(strconv.FormatBool(s.Registered)), -1),
			setDataRequest(path.Join("/instances", s.UUID, "name"), []byte(s.Name), -1),
			setDataRequest(path.Join("/instances", s.UUID, "version"), []byte(s.Version), -1),
			setDataRequest(path.Join("/instances", s.UUID, "region"), []byte(s.Region), -1),
			setDataRequest(path.Join("/instances", s.UUID, "addr"), []byte(s.ServiceAddr.String()), -1),
		},
	}

	err = sm.conn.Multi(ops)
	sm.managedInstances[s.UUID] = s

	return
}

func (sm *ZookeeperServiceManager) Remove(s skynet.ServiceInfo) (err error) {
	log.Println(log.TRACE, "Removing service", s.UUID)

	ops := zk.MultiOps{
		Delete: []zk.DeleteRequest{
			deleteRequest(path.Join("/regions", s.Region, s.UUID), -1),
			deleteRequest(path.Join("/services", s.Name, s.Version, s.UUID), -1),
			deleteRequest(path.Join("/services", s.Name, s.UUID), -1),
			deleteRequest(path.Join("/hosts", s.ServiceAddr.IPAddress, s.UUID), -1),

			deleteRequest(path.Join("/instances", s.UUID, "registered"), -1),
			deleteRequest(path.Join("/instances", s.UUID, "name"), -1),
			deleteRequest(path.Join("/instances", s.UUID, "version"), -1),
			deleteRequest(path.Join("/instances", s.UUID, "region"), -1),
			deleteRequest(path.Join("/instances", s.UUID, "addr"), -1),

			deleteRequest(path.Join("/instances", s.UUID), -1),
		},
	}

	err = sm.conn.Multi(ops)

	if err == nil {
		delete(sm.managedInstances, s.UUID)
	}

	// Attempt to remove parent paths for service if they are empty
	sm.removePathIfEmpty(path.Join("/hosts", s.ServiceAddr.IPAddress))
	sm.removePathIfEmpty(path.Join("/regions", s.Region))
	sm.removePathIfEmpty(path.Join("/services", s.Name))
	sm.removePathIfEmpty(path.Join("/services", s.Name, s.Version))

	return
}

func (sm *ZookeeperServiceManager) Register(uuid string) (err error) {
	log.Println(log.TRACE, "Registering service", uuid)

	_, err = sm.conn.Set(path.Join("/instances", uuid, "registered"), []byte("true"), -1)

	return
}

func (sm *ZookeeperServiceManager) Unregister(uuid string) (err error) {
	log.Println(log.TRACE, "Unregister service", uuid)

	_, err = sm.conn.Set(path.Join("/instances", uuid, "registered"), []byte("false"), -1)

	return
}

func (sm *ZookeeperServiceManager) Watch(criteria skynet.CriteriaMatcher, c chan<- skynet.InstanceNotification) (instances []skynet.ServiceInfo) {
	sm.watchers = append(sm.watchers, watcher{criteria, c})

	instances, _ = sm.cache.List(criteria)

	return
}

func (sm *ZookeeperServiceManager) notify(n skynet.InstanceNotification) {
	for _, w := range sm.watchers {
		if w.criteria.Matches(n.Service) {
			go func() {
				w.ch <- n
			}()
		}
	}
}

// Retrieve ServiceInfo from zookeeper
func (sm *ZookeeperServiceManager) getServiceInfo(uuid string) (s skynet.ServiceInfo, err error) {
	var b []byte

	s.ServiceConfig = new(skynet.ServiceConfig)
	s.UUID = uuid

	reg, _, err := sm.conn.Get(path.Join("/instances", uuid, "registered"))

	if err != nil {
		return
	}

	if string(reg) == "true" {
		s.Registered = true
	} else {
		s.Registered = false
	}

	addr, _, err := sm.conn.Get(path.Join("/instances", uuid, "addr"))

	if err != nil {
		return
	}

	s.ServiceAddr, err = skynet.BindAddrFromString(string(addr))

	if err != nil {
		return
	}

	b, _, err = sm.conn.Get(path.Join("/instances", uuid, "name"))

	if err != nil {
		return
	}

	s.Name = string(b)

	b, _, err = sm.conn.Get(path.Join("/instances", uuid, "version"))

	if err != nil {
		return
	}

	s.Version = string(b)

	b, _, err = sm.conn.Get(path.Join("/instances", uuid, "region"))

	if err != nil {
		return
	}

	s.Region = string(b)

	return
}

// Ensures defaults paths of /hosts, /instances, /services, /regions exist
// as all registrations are done in these paths
func (sm *ZookeeperServiceManager) createDefaultPaths() error {
	// We create all these paths with a MultiOp, if one exists they all exist
	exists, _, err := sm.conn.Exists("/instances")

	if exists || err != nil {
		return err
	}

	ops := zk.MultiOps{
		Create: []zk.CreateRequest{
			createRequest("/regions", []byte{}, zk.PermAll, 0),
			createRequest("/hosts", []byte{}, zk.PermAll, 0),
			createRequest("/services", []byte{}, zk.PermAll, 0),
			createRequest("/instances", []byte{}, zk.PermAll, 0),
		},
	}

	// We are throwing away error here, if it fails it's because the paths already exists
	return sm.conn.Multi(ops)
}

func (sm *ZookeeperServiceManager) createPathsForService(s skynet.ServiceInfo) error {
	_, err := sm.conn.Create(path.Join("/hosts", s.ServiceAddr.IPAddress), []byte{}, 0, zk.WorldACL(zk.PermAll))

	if err != nil && err != zk.ErrNodeExists {
		return err
	}

	_, err = sm.conn.Create(path.Join("/regions", s.Region), []byte{}, 0, zk.WorldACL(zk.PermAll))

	if err != nil && err != zk.ErrNodeExists {
		return err
	}

	_, err = sm.conn.Create(path.Join("/services", s.Name), []byte{}, 0, zk.WorldACL(zk.PermAll))

	if err != nil && err != zk.ErrNodeExists {
		return err
	}

	_, err = sm.conn.Create(path.Join("/services", s.Name, s.Version), []byte{}, 0, zk.WorldACL(zk.PermAll))

	if err != nil && err != zk.ErrNodeExists {
		return err
	}

	return nil
}

func (sm *ZookeeperServiceManager) removePathIfEmpty(path string) {
	_, stat, err := sm.conn.Children(path)

	if err == nil {
		if stat.NumChildren == 0 {

			// throw away errors here, if it error'd more than likely there are now new children
			sm.conn.Delete(path, stat.Version)
		}
	}
}

func createRequest(path string, data []byte, perms, flags int32) zk.CreateRequest {
	return zk.CreateRequest{
		Path:  path,
		Data:  data,
		Acl:   zk.WorldACL(perms),
		Flags: flags,
	}
}

func setDataRequest(path string, data []byte, version int32) zk.SetDataRequest {
	return zk.SetDataRequest{
		Path:    path,
		Data:    data,
		Version: version,
	}
}

func deleteRequest(path string, version int32) zk.DeleteRequest {
	return zk.DeleteRequest{
		Path:    path,
		Version: version,
	}
}

func (sm *ZookeeperServiceManager) mux() {
	for {
		select {
		case e := <-sm.session:
			switch e.Type {
			case zk.EventNodeDeleted, zk.EventNodeChildrenChanged, zk.EventNodeDataChanged:
			case zk.EventSession:
			// TODO: EventNotWatching
			// TODO: StateDisconnected
			default:
				log.Println(log.TRACE, "Zookeeper Event Received: ", e)
			}
		case <-sm.done:
			// Remove instances that were added by this instance
			for _, s := range sm.managedInstances {
				sm.Remove(s)
			}

			sm.cache.Stop()

			sm.conn.Close()
			return
		}
	}
}

func defaultFactory(servers []string, recvTimeout time.Duration) (ZkConnection, <-chan zk.Event, error) {
	return zk.Connect(servers, recvTimeout)
}
