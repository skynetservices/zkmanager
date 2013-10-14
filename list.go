package zkmanager

import (
	"github.com/skynetservices/skynet"
)

// Return a list of service versions that match criteria
func (sm *ZookeeperServiceManager) ListInstances(c skynet.CriteriaMatcher) (instances []skynet.ServiceInfo, err error) {
	return sm.cache.List(c)
}

// Return a list of service versions that match criteria
func (sm *ZookeeperServiceManager) ListVersions(c skynet.CriteriaMatcher) (versions []string, err error) {
	keys := make(map[string]bool)

	instances, err := sm.cache.List(c)

	if err != nil {
		return
	}

	for _, i := range instances {
		if _, ok := keys[i.Version]; !ok {
			keys[i.Version] = true
			versions = append(versions, i.Version)
		}
	}

	return
}

// Return a list of regions that match criteria
func (sm *ZookeeperServiceManager) ListRegions(c skynet.CriteriaMatcher) (regions []string, err error) {
	keys := make(map[string]bool)

	instances, err := sm.cache.List(c)

	if err != nil {
		return
	}

	for _, i := range instances {
		if _, ok := keys[i.Region]; !ok {
			keys[i.Region] = true
			regions = append(regions, i.Region)
		}
	}

	return
}

// Return a list of services that match criteria
func (sm *ZookeeperServiceManager) ListServices(c skynet.CriteriaMatcher) (services []string, err error) {
	keys := make(map[string]bool)

	instances, err := sm.cache.List(c)

	if err != nil {
		return
	}

	for _, i := range instances {
		if _, ok := keys[i.Name]; !ok {
			keys[i.Name] = true
			services = append(services, i.Name)
		}
	}

	return
}

// Return a list of hosts that match criteria
func (sm *ZookeeperServiceManager) ListHosts(c skynet.CriteriaMatcher) (hosts []string, err error) {
	keys := make(map[string]bool)

	instances, err := sm.cache.List(c)

	if err != nil {
		return
	}

	for _, i := range instances {
		if _, ok := keys[i.ServiceAddr.String()]; !ok {
			keys[i.ServiceAddr.String()] = true
			hosts = append(hosts, i.ServiceAddr.String())
		}
	}

	return
}
