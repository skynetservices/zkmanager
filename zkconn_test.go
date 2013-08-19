package zkmanager

import (
	"github.com/samuel/go-zookeeper/zk"
)

type TestConnection struct {
	AddAuthFunc                            func(scheme string, auth []byte) error
	ChildrenFunc                           func(path string) ([]string, *zk.Stat, error)
	ChildrenWFunc                          func(path string) ([]string, *zk.Stat, <-chan zk.Event, error)
	CloseFunc                              func()
	CreateFunc                             func(path string, data []byte, flags int32, acl []zk.ACL) (string, error)
	CreateProtectedEphemeralSequentialFunc func(path string, data []byte, acl []zk.ACL) (string, error)
	DeleteFunc                             func(path string, version int32) error
	ExistsFunc                             func(path string) (bool, *zk.Stat, error)
	ExistsWFunc                            func(path string) (bool, *zk.Stat, <-chan zk.Event, error)
	GetFunc                                func(path string) ([]byte, *zk.Stat, error)
	GetACLFunc                             func(path string) ([]zk.ACL, *zk.Stat, error)
	GetWFunc                               func(path string) ([]byte, *zk.Stat, <-chan zk.Event, error)
	MultiFunc                              func(ops zk.MultiOps) error
	SetFunc                                func(path string, data []byte, version int32) (*zk.Stat, error)
	SetACLFunc                             func(path string, acl []zk.ACL, version int32) (*zk.Stat, error)
	StateFunc                              func() zk.State
	SyncFunc                               func(path string) (string, error)
}

func (c *TestConnection) AddAuth(scheme string, auth []byte) error {
	if c.AddAuthFunc != nil {
		return c.AddAuthFunc(scheme, auth)
	}

	return nil
}

func (c *TestConnection) Children(path string) ([]string, *zk.Stat, error) {
	if c.ChildrenFunc != nil {
		return c.ChildrenFunc(path)
	}

	return []string{}, nil, nil
}

func (c *TestConnection) ChildrenW(path string) ([]string, *zk.Stat, <-chan zk.Event, error) {
	if c.ChildrenWFunc != nil {
		return c.ChildrenWFunc(path)
	}

	return []string{}, nil, make(chan zk.Event), nil
}

func (c *TestConnection) Close() {
	if c.CloseFunc != nil {
		c.CloseFunc()
	}
}

func (c *TestConnection) Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error) {
	if c.CreateFunc != nil {
		return c.CreateFunc(path, data, flags, acl)
	}

	return "", nil
}

func (c *TestConnection) CreateProtectedEphemeralSequential(path string, data []byte, acl []zk.ACL) (string, error) {
	if c.CreateProtectedEphemeralSequentialFunc != nil {
		return c.CreateProtectedEphemeralSequentialFunc(path, data, acl)
	}

	return "", nil
}

func (c *TestConnection) Delete(path string, version int32) error {
	if c.DeleteFunc != nil {
		return c.DeleteFunc(path, version)
	}

	return nil
}

func (c *TestConnection) Exists(path string) (bool, *zk.Stat, error) {
	if c.ExistsFunc != nil {
		return c.ExistsFunc(path)
	}

	return false, nil, nil
}

func (c *TestConnection) ExistsW(path string) (bool, *zk.Stat, <-chan zk.Event, error) {
	if c.ExistsWFunc != nil {
		return c.ExistsWFunc(path)
	}

	return false, nil, make(chan zk.Event), nil
}

func (c *TestConnection) Get(path string) ([]byte, *zk.Stat, error) {
	if c.GetFunc != nil {
		return c.GetFunc(path)
	}

	return []byte{}, nil, nil
}

func (c *TestConnection) GetACL(path string) ([]zk.ACL, *zk.Stat, error) {
	if c.GetACLFunc != nil {
		return c.GetACLFunc(path)
	}

	return []zk.ACL{}, nil, nil
}

func (c *TestConnection) GetW(path string) ([]byte, *zk.Stat, <-chan zk.Event, error) {
	if c.GetWFunc != nil {
		return c.GetWFunc(path)
	}

	return []byte{}, nil, make(chan zk.Event), nil
}

func (c *TestConnection) Multi(ops zk.MultiOps) error {
	if c.MultiFunc != nil {
		return c.MultiFunc(ops)
	}

	return nil
}

func (c *TestConnection) Set(path string, data []byte, version int32) (*zk.Stat, error) {
	if c.SetFunc != nil {
		return c.SetFunc(path, data, version)
	}

	return nil, nil
}

func (c *TestConnection) SetACL(path string, acl []zk.ACL, version int32) (*zk.Stat, error) {
	if c.SetACLFunc != nil {
		return c.SetACLFunc(path, acl, version)
	}

	return nil, nil
}

func (c *TestConnection) State() zk.State {
	if c.StateFunc != nil {
		return c.StateFunc()
	}

	return zk.StateConnected
}

func (c *TestConnection) Sync(path string) (string, error) {
	if c.SyncFunc != nil {
		return c.SyncFunc(path)
	}

	return "", nil
}
