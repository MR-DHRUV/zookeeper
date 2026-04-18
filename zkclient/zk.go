package zkclient

import (
	"log"
	"sync"
	"time"

	"github.com/go-zookeeper/zk"
)

var (
	zkClient *zk.Conn
	once     sync.Once
	initErr  error
)

var servers = []string{
	"127.0.0.1:2181",
	"127.0.0.1:2182",
	"127.0.0.1:2183",
}

// 🔹 Singleton accessor
func GetZkClient() (*zk.Conn, error) {
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

		zkClient = conn
	})

	return zkClient, initErr
}
