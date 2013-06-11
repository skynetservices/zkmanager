package zkmanager

import (
	"github.com/petar/gozk"
	"github.com/skynetservices/skynet2"
	"path"
)

// Return a list of service versions that match criteria
func (sm *ZookeeperServiceManager) ListVersions(c skynet.Criteria) (versions []string, err error) {
	children, _, err := sm.conn.Children("/services")
	for _, child := range children {
		var stat *zookeeper.Stat

		_, stat, err = sm.conn.Get(path.Join("/services", child))

		// UUIDs wont have children
		if stat.NumChildren() > 0 {
			var versionNodes = make([]string, 0, 0)
			versionNodes, _, err = sm.conn.Children(path.Join("/services", child))

			if err != nil {
				return
			}

			for _, version := range versionNodes {
				if sm.hasMatchingInstances(path.Join("/services", child, version), c) {
					versions = append(versions, version)
				}
			}
		}
	}

	return
}

// Return a list of regions that match criteria
func (sm *ZookeeperServiceManager) ListRegions(c skynet.Criteria) (regions []string, err error) {
	return sm.pathsWithMatchingInstances("/regions", c)
}

// Return a list of services that match criteria
func (sm *ZookeeperServiceManager) ListServices(c skynet.Criteria) (services []string, err error) {
	return sm.pathsWithMatchingInstances("/services", c)
}

// Return a list of hosts that match criteria
func (sm *ZookeeperServiceManager) ListHosts(c skynet.Criteria) (hosts []string, err error) {
	return sm.pathsWithMatchingInstances("/hosts", c)
}

func (sm *ZookeeperServiceManager) pathsWithMatchingInstances(basePath string, c skynet.Criteria) (paths []string, err error) {
	stat, err := sm.conn.Exists(basePath)

	if err != nil {
		return
	}

	if stat.NumChildren() < 1 {
		return
	}

	children, _, err := sm.conn.Children(basePath)

	if err != nil {
		return
	}

	for _, child := range children {
		var stat *zookeeper.Stat
		_, stat, err = sm.conn.Get(path.Join(basePath, child))

		if err != nil {
			return nil, err
		}

		if stat.NumChildren() > 0 {
			var uuids []string
			uuids, _, err = sm.conn.Children(path.Join(basePath, child))
			if err != nil {
				return
			}

			for _, uuid := range uuids {
				if uuid == "" {
					continue
				}

				var instance skynet.ServiceInfo
				instance, _ = sm.getServiceInfo(uuid)

				if c.Matches(instance) {
					paths = append(paths, child)

					// We only need 1 instance to match, let's skip retrieving the rest
					break // uuid loop
				}
			}
		}
	}

	return
}

func (sm *ZookeeperServiceManager) hasMatchingInstances(basePath string, c skynet.Criteria) bool {
	uuids, _, err := sm.conn.Children(basePath)
	if err != nil {
		return false
	}

	for _, uuid := range uuids {
		if uuid == "" {
			continue
		}

		var instance skynet.ServiceInfo
		instance, _ = sm.getServiceInfo(uuid)

		if c.Matches(instance) {
			// We only need 1 instance to match, let's skip retrieving the rest
			return true
		}
	}

	return false
}
