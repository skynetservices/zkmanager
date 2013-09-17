package zkmanager

import (
	"errors"
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

	c.buildInitialCache()

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
			case PathCacheAddNotification, PathCacheUpdateNotification:
				s, err := c.getServiceInfo(uuid)

				// err means not all paths exist yet
				if err != nil {
					continue
				}

				if _, ok := c.instances[uuid]; ok {
					log.Println(log.TRACE, "InstanceCache instance updated:", uuid)
					c.instances[uuid] = s
					go c.notify(skynet.InstanceUpdated, s)
				} else {
					log.Println(log.TRACE, "InstanceCache instance added:", uuid)
					c.instances[uuid] = s
					go c.notify(skynet.InstanceAdded, s)
				}

			case PathCacheRemoveNotification:
				if n.Path == path.Join(InstancesBasePath, uuid) {
					if s, ok := c.instances[uuid]; ok {
						log.Println(log.TRACE, "InstanceCache instance removed:", uuid)
						go c.notify(skynet.InstanceRemoved, s)

						delete(c.instances, uuid)
					}
				}
			}
		}
	}
}

func (c *InstanceCache) notify(typ int, s skynet.ServiceInfo) {
	c.serviceManager.notify(skynet.InstanceNotification{Type: typ, Service: s})
}

// we return error if anything is missing so instance adds aren't triggered before all paths have been created
func (c *InstanceCache) getServiceInfo(uuid string) (s skynet.ServiceInfo, err error) {
	s.UUID = uuid

	if name := c.cache.PathValue(path.Join(InstancesBasePath, uuid, "name")); len(name) > 0 {
		s.Name = string(name)
	} else {
		return s, errors.New("name missing for: " + uuid)
	}

	if region := c.cache.PathValue(path.Join(InstancesBasePath, uuid, "region")); len(region) > 0 {
		s.Region = string(region)
	} else {
		return s, errors.New("region missing for: " + uuid)
	}

	if version := c.cache.PathValue(path.Join(InstancesBasePath, uuid, "version")); len(version) > 0 {
		s.Version = string(version)
	} else {
		return s, errors.New("version missing for: " + uuid)
	}

	if registered := c.cache.PathValue(path.Join(InstancesBasePath, uuid, "registered")); len(registered) > 0 {
		if string(registered) == "true" {
			s.Registered = true
		} else {
			s.Registered = false
		}
	} else {
		return s, errors.New("registered missing for: " + uuid)
	}

	if addr := c.cache.PathValue(path.Join(InstancesBasePath, uuid, "addr")); len(addr) > 0 {
		s.ServiceAddr, err = skynet.BindAddrFromString(string(addr))
	} else {
		return s, errors.New("addr missing for: " + uuid)
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

func (c *InstanceCache) buildInitialCache() {
	for _, p := range c.cache.Children() {
		uuid := uuidFromPath(p)

		s, err := c.getServiceInfo(uuid)

		if err != nil {
			log.Println(log.WARN, err)
			continue
		}

		c.instances[uuid] = s
	}
}

func uuidFromPath(path string) (uuid string) {
	parts := strings.Split(path, "/")

	if len(parts) < 3 {
		return
	}

	return parts[2]
}
