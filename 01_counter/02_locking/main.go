package main

import (
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-zookeeper/zk"
	"zookeeper/zkclient"
)

func main() {
	conn, err := zkclient.GetZkClient()
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	wg := sync.WaitGroup{}
	var totalRetries int64

	for i := 0; i < 1000; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			retry := 0

			// Create ephemeral sequential node
			path, err := conn.Create(
				"/lock/lock-",
				[]byte(""),
				zk.FlagEphemeral|zk.FlagSequence,
				zk.WorldACL(zk.PermAll),
			)
			if err != nil {
				log.Println("create error:", err)
				return
			}

			nodeName := path[strings.LastIndex(path, "/")+1:]
			for {
				children, _, err := conn.Children("/lock")
				if err != nil {
					continue
				}

				sort.Strings(children)

				// If smallest node → acquire lock
				if children[0] == nodeName {
					break
				}

				// Find previous node
				var prevNode string
				for i := 0; i < len(children); i++ {
					if children[i] == nodeName && i > 0 {
						prevNode = children[i-1]
						break
					}
				}

				// Watch previous node
				_, _, ch, err := conn.ExistsW("/lock/" + prevNode)
				if err != nil {
					continue
				}

				retry++

				select {
				case <-ch:
					// previous node deleted → retry
				case <-time.After(time.Duration(retry*100) * time.Millisecond):
					// small jitter fallback expo backoff
				}
			}

			// ===== CRITICAL SECTION =====
			for {
				data, stat, err := conn.Get("/val")
				if err != nil {
					continue
				}

				val, err := strconv.Atoi(string(data))
				if err != nil {
					continue
				}

				_, err = conn.Set("/val", []byte(strconv.Itoa(val+1)), stat.Version)
				if err == nil {
					break
				}
			}
			// ============================

			// Release lock
			err = conn.Delete(path, -1)
			if err != nil {
				log.Println("delete error:", err)
			}

			atomic.AddInt64(&totalRetries, int64(retry))
		}()
	}

	wg.Wait()

	data, _, err := conn.Get("/val")
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Final value: %s", string(data))
	log.Printf("Total retries: %d", totalRetries)
}

/*
Results: 

1. Exponential Backoff (No Lock, last result):
   - Final value: 1001
   - Retries: ~27,000
   - Time: ~13s
   - Behavior:
     Uses optimistic concurrency (CAS). High contention still causes many
     conflicts, but backoff reduces retry storms and improves throughput.

2. ZK Lock (Ephemeral Sequential):
   - Final value: 1001
   - Retries: ~10,400
   - Time: ~11s
   - Behavior:
     Serializes access via queue (no write conflicts). Stable and predictable,
     each client waits for its turn.

Summary:
- Backoff = higher parallelism but more conflicts
- Lock = fewer retries, no conflicts, but fully serialized

Locks perform better under heavy contention, while backoff is simpler and
works well when contention is moderate.
*/