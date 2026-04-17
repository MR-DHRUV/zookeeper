package main

import (
	"log"
	"strconv"
	"sync"
	"zookeeper/zkclient"
)

func main() {
	zk, err := zkclient.GetZkClient()
	if err != nil {
		return
	}
	defer zk.Close()

	wg := sync.WaitGroup{}
	totalReties := 0

	// launch 1k go routines to update /val
	for i := 1; i <= 100; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			retry := 0
			for {
				data, stat, err := zk.Get("/val")
				if err != nil {
					continue
				}

				// parse data to int
				val, err := strconv.Atoi(string(data))
				if err != nil {
					continue
				}

				if _, err = zk.Set("/val", []byte(strconv.Itoa(val+1)), stat.Version); err == nil {
					break
				}

				retry++
			}

			totalReties += retry
		}()
	}

	wg.Wait()

	// read final value
	data, _, err := zk.Get("/val")
	if err != nil {
		return
	}

	log.Printf("Final value: %s", string(data))
	log.Printf("Total retries: %d", totalReties)
}

/*
Results:

1. High contention scenario (1000 concurrent updates)
   Final value: 1001
   Total retries: 250,594

2. Moderate contention scenario (100 concurrent updates)
   Final value: 101
   Total retries: 2,663


Analysis:
- Each update follows a read-modify-write pattern using ZooKeeper's version-based optimistic locking.
- Under high concurrency, multiple clients read the same version and attempt to write,
  causing frequent version conflicts and retries.
- ZooKeeper guarantees strong consistency, so conflicting writes are rejected rather than merged.


Conclusion:
- ZooKeeper ensures correctness (no lost updates), but performs poorly under heavy write contention.
- Retry count increases non-linearly with concurrency due to contention on a single znode.
- This approach is not suitable for high-throughput counters or shared mutable state.
*/