package zkmanager

import (
	"github.com/skynetservices/skynet2"
	"github.com/skynetservices/skynet2/log"
	"path"
	"strings"
)

const (
	InstancesBasePath = "/instances"
	RegionsBasePath   = "/regions"
	ServicesBasePath  = "/services"
	HostsBasePath     = "/hosts"
)

type InstanceCache struct {
	cache      *PathCache
	pathNotify chan PathCacheNotification
	instances  map[string]skynet.ServiceInfo

	serviceManager *ZookeeperServiceManager
}

func NewInstanceCache(sm *ZookeeperServiceManager) (*InstanceCache, error) {
	pc, ev, err := NewPathCache(InstancesBasePath, 2, sm)

	c := &InstanceCache{
		cache:          pc,
		pathNotify:     ev,
		instances:      make(map[string]skynet.ServiceInfo),
		serviceManager: sm,
	}

	go c.watch()

	return c, err
}

func (c *InstanceCache) watch() {
	for {
		select {
		case n, ok := <-c.pathNotify:
			if !ok {
				return
			}

			uuid := uuidFromPath(n.Path)

			switch n.Type {
			case PathCacheAddNotification:
				if s, err := c.getServiceInfo(uuid); err != nil {
					c.instances[uuid] = s
					go c.notify(skynet.InstanceAdded, s)
				}

			case PathCacheUpdateNotification:
				if s, err := c.getServiceInfo(uuid); err != nil {
					c.instances[uuid] = s
					go c.notify(skynet.InstanceUpdated, s)
				}

			case PathCacheRemoveNotification:
				if s, ok := c.instances[uuid]; ok {
					go c.notify(skynet.InstanceRemoved, s)

					delete(c.instances, uuid)
				}
			}
		}
	}
}

func (c *InstanceCache) notify(typ int, s skynet.ServiceInfo) {
	c.serviceManager.notify(skynet.InstanceNotification{Type: typ, Service: s})
}

func (c *InstanceCache) getServiceInfo(uuid string) (s skynet.ServiceInfo, err error) {
	s.ServiceConfig = new(skynet.ServiceConfig)
	s.UUID = uuid

	if name := c.cache.PathValue(path.Join(InstancesBasePath, uuid, "name")); len(name) > 0 {
		s.Name = string(name)
	}

	if region := c.cache.PathValue(path.Join(InstancesBasePath, uuid, "region")); len(region) > 0 {
		s.Region = string(region)
	}

	if version := c.cache.PathValue(path.Join(InstancesBasePath, uuid, "version")); len(version) > 0 {
		s.Version = string(version)
	}

	if registered := c.cache.PathValue(path.Join(InstancesBasePath, uuid, "registered")); len(registered) > 0 {
		if string(registered) == "true" {
			s.Registered = true
		} else {
			s.Registered = false
		}
	}

	if addr := c.cache.PathValue(path.Join(InstancesBasePath, uuid, "addr")); len(addr) > 0 {
		s.ServiceAddr, err = skynet.BindAddrFromString(string(addr))
	}

	return
}

func (c *InstanceCache) List(criteria skynet.CriteriaMatcher) (instances []skynet.ServiceInfo, err error) {
	for _, s := range c.instances {
		if criteria.Matches(s) {
			instances = append(instances, s)
		}
	}

	return
}

func (c *InstanceCache) Stop() {
	c.cache.Stop()
}

func uuidFromPath(path string) (uuid string) {
	parts := strings.Split(path, "/")

	if len(parts) < 3 {
		return
	}

	return parts[2]
}
