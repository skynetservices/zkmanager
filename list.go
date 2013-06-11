package zkmanager

import (
	"github.com/petar/gozk"
	"github.com/skynetservices/skynet2"
	"path"
)

// Return a list of hosts that match criteria
func (sm *ZookeeperServiceManager) ListHosts(c skynet.Criteria) (hosts []string, err error) {
	stat, err := sm.conn.Exists("/hosts")

	if err != nil {
		return
	}

	if stat.NumChildren() < 1 {
		return
	}

	children, _, err := sm.conn.Children("/hosts")

	if err != nil {
		return
	}

	for _, host := range children {
		var stat *zookeeper.Stat
		_, stat, err = sm.conn.Get(path.Join("/hosts", host))

		if err != nil {
			return nil, err
		}

		if stat.NumChildren() > 0 {
			var uuids []string
			uuids, _, err = sm.conn.Children(path.Join("/hosts", host))
			if err != nil {
				return
			}

			for _, uuid := range uuids {
				var instance skynet.ServiceInfo
				instance, _ = sm.getServiceInfo(uuid)

				if c.Matches(instance) {
					hosts = append(hosts, host)

					// We only need 1 instance to match, let's skip retrieving the rest
					break // uuid loop
				}
			}
		}
	}

	return
}
