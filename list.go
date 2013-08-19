package zkmanager

import (
	"github.com/skynetservices/skynet2"
	"strings"
)

// Return a list of service versions that match criteria
func (sm *ZookeeperServiceManager) ListInstances(c skynet.CriteriaMatcher) (instances []skynet.ServiceInfo, err error) {
	// TODO: Let's try for a better approach than iterating over all instances that exist
	// this may require the CriteriaMatcher to implement more methods to provide us lists of Hosts, Services, Regions, etc.
	for _, instance := range sm.instanceCache {
		if c.Matches(instance) {
			instances = append(instances, instance)
		}
	}

	return
}

// Return a list of service versions that match criteria
func (sm *ZookeeperServiceManager) ListVersions(c skynet.CriteriaMatcher) (versions []string, err error) {
	for name, instances := range sm.serviceCache {
		// ingore standard name with no version
		if strings.Contains(name, "::") {
			parts := strings.Split(name, "::")
			version := parts[1]

			if sm.hasMatchingInstances(instances, c) {
				versions = append(versions, version)
			}
		}
	}

	return
}

// Return a list of regions that match criteria
func (sm *ZookeeperServiceManager) ListRegions(c skynet.CriteriaMatcher) (regions []string, err error) {
	for region, instances := range sm.regionCache {
		if sm.hasMatchingInstances(instances, c) {
			regions = append(regions, region)
		}
	}

	return
}

// Return a list of services that match criteria
func (sm *ZookeeperServiceManager) ListServices(c skynet.CriteriaMatcher) (services []string, err error) {
	for name, instances := range sm.serviceCache {
		// ingore entries that contain versions
		if !strings.Contains(name, "::") {
			if sm.hasMatchingInstances(instances, c) {
				services = append(services, name)
			}
		}
	}

	return
}

// Return a list of hosts that match criteria
func (sm *ZookeeperServiceManager) ListHosts(c skynet.CriteriaMatcher) (hosts []string, err error) {
	for host, instances := range sm.hostCache {
		if sm.hasMatchingInstances(instances, c) {
			hosts = append(hosts, host)
		}
	}
	return
}

func (sm *ZookeeperServiceManager) hasMatchingInstances(uuids []string, c skynet.CriteriaMatcher) bool {
	for _, uuid := range uuids {
		if instance, ok := sm.instanceCache[uuid]; ok {
			if c.Matches(instance) {
				// We only need 1 instance to match, let's skip retrieving the rest
				return true
			}
		}
	}

	return false
}
