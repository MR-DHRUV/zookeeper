package zkclient

import (
	"log"
	"sync"
	"time"

	"github.com/go-zookeeper/zk"
)

type Client struct {
	Conn *zk.Conn
}

var (
	zkClient *Client
	once     sync.Once
	initErr  error
)

var servers = []string{
	"127.0.0.1:2181",
	"127.0.0.1:2182",
	"127.0.0.1:2183",
}

// 🔹 Singleton accessor
func GetZkClient() (*Client, error) {
	once.Do(func() {
		conn, events, err := zk.Connect(servers, 5*time.Second)
		if err != nil {
			initErr = err
			return
		}

		log.Println("Zk connected")

		// Event logger (super useful)
		go func() {
			for event := range events {
				log.Printf("[ZK EVENT] State=%s Type=%s Path=%s\n",
					event.State, event.Type, event.Path)
			}
		}()

		zkClient = &Client{
			Conn: conn,
		}
	})

	return zkClient, initErr
}

func Reset() {
	if zkClient != nil && zkClient.Conn != nil {
		zkClient.Conn.Close()
	}
	zkClient = nil
	initErr = nil
	once = sync.Once{}
}

// Close connection
func (c *Client) Close() {
	if c.Conn != nil {
		c.Conn.Close()
	}
}

func (c *Client) Create(path string, data []byte, flags int32) (string, error) {
	return c.Conn.Create(path, data, flags, zk.WorldACL(zk.PermAll))
}

func (c *Client) Get(path string) ([]byte, *zk.Stat, error) {
	return c.Conn.Get(path)
}

func (c *Client) Set(path string, data []byte, version int32) (*zk.Stat, error) {
	return c.Conn.Set(path, data, version)
}

func (c *Client) Delete(path string, version int32) error {
	return c.Conn.Delete(path, version)
}

func (c *Client) Exists(path string) (bool, *zk.Stat, error) {
	return c.Conn.Exists(path)
}

func (c *Client) Children(path string) ([]string, *zk.Stat, error) {
	return c.Conn.Children(path)
}

func (c *Client) GetW(path string) ([]byte, *zk.Stat, <-chan zk.Event, error) {
	return c.Conn.GetW(path)
}

func (c *Client) ExistsW(path string) (bool, *zk.Stat, <-chan zk.Event, error) {
	return c.Conn.ExistsW(path)
}

func (c *Client) ChildrenW(path string) ([]string, *zk.Stat, <-chan zk.Event, error) {
	return c.Conn.ChildrenW(path)
}
