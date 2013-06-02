package zkmanager

import (
	"github.com/petar/gozk"
	"github.com/skynetservices/skynet"
	"github.com/skynetservices/skynet/log"
	"strings"
	"time"
)

/* TODO: Lot's of testing and error handling */
type ZookeeperServiceManager struct {
	conn *zookeeper.Conn
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

	return &ZookeeperServiceManager{
		conn: zk,
	}
}

func (sm *ZookeeperServiceManager) Add(s skynet.ServiceInfo) {
	log.Println(log.TRACE, "Adding service to cluster", s.Config.UUID)

	sm.addService(s)
	sm.createPaths(s)
}

func (sm *ZookeeperServiceManager) Update(s skynet.ServiceInfo) {
	log.Println(log.TRACE, "Updating service", s.Config.UUID)
}

func (sm *ZookeeperServiceManager) Remove(uuid string) {
	log.Println(log.TRACE, "Removing service", uuid)
}

func (sm *ZookeeperServiceManager) Register(uuid string) {
	log.Println(log.TRACE, "Registering service", uuid)

	sm.conn.Set("/instances/"+uuid+"/registered", "1", -1)
}

func (sm *ZookeeperServiceManager) Unregister(uuid string) {
	log.Println(log.TRACE, "Unregister service", uuid)
	sm.conn.Set("/instances/"+uuid+"/registered", "0", -1)
}

func (sm *ZookeeperServiceManager) addService(s skynet.ServiceInfo) {
	sm.createPath("/instances/" + s.Config.UUID)
	sm.conn.Create("/instances/"+s.Config.UUID+"/registered", "0", zookeeper.EPHEMERAL, zookeeper.WorldACL(zookeeper.PERM_ALL))
	sm.conn.Create("/instances/"+s.Config.UUID+"/addr", s.Config.ServiceAddr.String(), zookeeper.EPHEMERAL, zookeeper.WorldACL(zookeeper.PERM_ALL))
}

func (sm *ZookeeperServiceManager) createPaths(s skynet.ServiceInfo) {
	// Add UUID to /regions
	sm.createPath("/regions/" + s.Config.Region)
	sm.conn.Create("/regions/"+s.Config.Region+"/"+s.Config.UUID, "", zookeeper.EPHEMERAL, zookeeper.WorldACL(zookeeper.PERM_ALL))

	// Add UUID to /services/ServiceName and /services/ServiceName/Version
	sm.createPath("/services/" + s.Config.Name + "/" + s.Config.Version)
	sm.conn.Create("/services/"+s.Config.Name+"/"+s.Config.UUID, "", zookeeper.EPHEMERAL, zookeeper.WorldACL(zookeeper.PERM_ALL))
	sm.conn.Create("/services/"+s.Config.Name+"/"+s.Config.Version+"/"+s.Config.UUID, "", zookeeper.EPHEMERAL, zookeeper.WorldACL(zookeeper.PERM_ALL))

	// Add UUID to /hosts/IPAddress
	sm.createPath("/hosts/" + s.Config.ServiceAddr.IPAddress)
	sm.conn.Create("/hosts/"+s.Config.ServiceAddr.IPAddress+"/"+s.Config.UUID, "", zookeeper.EPHEMERAL, zookeeper.WorldACL(zookeeper.PERM_ALL))
}

func (sm *ZookeeperServiceManager) createPath(path string) error {
	parts := strings.Split(path, "/")
	path = ""

	for _, p := range parts {
		if p == "" {
			continue
		}

		path = path + "/" + p

		if stat, _ := sm.conn.Exists(path); stat != nil {
			log.Println(log.DEBUG, "ZK path exists: "+path)
			continue
		}

		log.Println(log.TRACE, "Creating ZK path: "+path)

		_, err := sm.conn.Create(path, "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))

		if err != nil {
			log.Println(log.ERROR, err.Error())
			return err
		}
	}

	return nil
}
