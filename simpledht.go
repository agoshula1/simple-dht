// implementation of a dht with consistent hashing
// alteration and expansion on an implementation in the groupcache repository:
// https://github.com/golang/groupcache/blob/master/consistenthash/consistenthash.go
package main

import (
    "github.com/fanliao/go-concurrentMap"
    "fmt"
    "hash/fnv"
    "sort"
    "sync"
    "math/rand"
    "strconv"
)

type Node struct {
  alive bool // true if node is functional (false if it has crashed)
  aliveMux *sync.RWMutex
  localMap *concurrent.ConcurrentMap
}

func NewNode() *Node {
	n := &Node{
		alive: true,
    aliveMux: &sync.RWMutex{},
    localMap: concurrent.NewConcurrentMap(),
	}
	return n
}

// called when node has crashed or recovered
func (n *Node) updateLiveness(isAlive bool) {
  n.aliveMux.Lock()
  defer n.aliveMux.Unlock()
  n.alive = isAlive
}

// returns true if node is functional
func (n *Node) isAlive() bool {
  n.aliveMux.RLock()
  defer n.aliveMux.RUnlock()
  return n.alive
}

// hash function to find position of virtual nodes and keys on ring
func hash(s string) int {
  h := fnv.New32a()
  h.Write([]byte(s))
  return int(h.Sum32())
}

type RingHash struct {
  vNodes []int //virtual nodes
  vNodesMux *sync.RWMutex
  numVirtuals int //number of virtual nodes per real node
  nodes *concurrent.ConcurrentMap //actual nodes
  hashMap *concurrent.ConcurrentMap //mapping from virtual node to corresponding real node
  db *concurrent.ConcurrentMap //representing data base server
}

func NewRing(virtuals int) *RingHash {
	r := &RingHash{
		vNodesMux: &sync.RWMutex{},
    numVirtuals: virtuals,
    nodes: concurrent.NewConcurrentMap(),
		hashMap:  concurrent.NewConcurrentMap(),
    db: concurrent.NewConcurrentMap(),
	}
	return r
}

// Returns true if there are no available nodes
func (r *RingHash) IsEmpty() bool {
	for itr := r.nodes.Iterator();itr.HasNext(); {
  	_, node, _ := itr.Next()
    // inactive nodes may still be recorded in system
    if node.(*Node).isAlive(){
      return false
    }
  }
  return true
}

// Adds some nodes to the ring
func (r *RingHash) Add(nodes ...string) {
  for _, node := range nodes {
    // check if node is already present
    if existingNode, _ := r.nodes.Get(node); existingNode != nil{
      if !existingNode.(*Node).isAlive(){
        // reactivate node
        existingNode.(*Node).updateLiveness(true)
        fmt.Printf("%s reactivated\n", node)
      }
    } else {
      // create node
      r.nodes.Put(node, NewNode())

      //create corresponding virtual nodes
      for i := 0; i < r.numVirtuals; i++ {
  			hash := hash(strconv.Itoa(i) + node)
        // safely access virtual nodes list
        r.vNodesMux.Lock()
  			r.vNodes = append(r.vNodes, hash)
        r.vNodesMux.Unlock()
  			r.hashMap.Put(hash, node)
  		}
    }
	}
  // safely access virtual nodes list
  r.vNodesMux.Lock()
  sort.Ints(r.vNodes)
  r.vNodesMux.Unlock()
}

// Deactivate node in ring
func (r *RingHash) Remove(node string) {
  if existingNode, _ := r.nodes.Get(node); existingNode != nil && existingNode.(*Node).isAlive(){
    // deactivate node
    existingNode.(*Node).updateLiveness(false)
    fmt.Printf("%s deactivated\n", node)
  }
}

// Gets the closest active node in the ring to the provided key
func (r *RingHash) get(key string) string {
	if r.IsEmpty() {
		return ""
	}

	hash := hash(key)

	// Binary search for appropriate virtual node.
  // safely access virtual nodes list
  r.vNodesMux.RLock()
  defer r.vNodesMux.RUnlock()
	idx := sort.Search(len(r.vNodes), func(i int) bool { return r.vNodes[i] >= hash })

	// Means we have cycled back to the first virtual node.
	if idx == len(r.vNodes) {
		idx = 0
	}

  nodeId, _ := r.hashMap.Get(r.vNodes[idx])
  node, _ := r.nodes.Get(nodeId)
  // find next active node
  for ; !node.(*Node).isAlive(); {
    idx++ // virtual nodes are in sorted ascending order

    // Means we have cycled back to the first virtual node.
    if idx == len(r.vNodes) {
      idx = 0
    }
    nodeId, _ = r.hashMap.Get(r.vNodes[idx])
    node, _ = r.nodes.Get(nodeId)
  }

	return nodeId.(string)
}

// Get the value mapped to the key
func (r *RingHash) Get(key string) (int, bool) {
  nodeId := r.get(key)

  if len(nodeId) > 0{
    // check if key exists in node
    node, _ := r.nodes.Get(nodeId)
    value, _ := node.(*Node).localMap.Get(key)
    if value == nil{
      val, _ := r.db.Get(key)
      if val == nil{
        return -1, false
      }

      // add (key, value) pair to node
      node, _ = r.nodes.Get(nodeId)
      node.(*Node).localMap.Put(key, val)
      return val.(int), true
    }

    return value.(int), true
  }
  return -1, false
}

// Put the (key, value) pair in ring
func (r *RingHash) Put(key string, value int) {
  // add to db server
  r.db.Put(key, value)

  // add to closest node
  nodeId := r.get(key)
  if len(nodeId) > 0{
    node, _ := r.nodes.Get(nodeId)
    node.(*Node).localMap.Put(key, value)
  }
}

// test effects of deactivating and reactivating same node
func basictest() {
  // initialize ring and nodes
  ring := NewRing(2) //two virtual nodes per real node

  // three real nodes
  for i := 0; i < 3; i++ {
    id := "node" + strconv.Itoa(i)
    ring.Add(id)
  }

  // ten keys
  var wait sync.WaitGroup
  wait.Add(10)
  for i := 0; i < 10; i++ {
    go func(ind int){
      ring.Put("key" + strconv.Itoa(ind), ind)
      wait.Done()
    }(i)
  }

  nodeId := "node0"
  // deactivate node0
  ring.Remove(nodeId)

  // key0 should be mapped to a different node
  if ring.get("key0") == nodeId{
    fmt.Println("Error: node not properly deactivated\n")
  }
  if val, found := ring.Get("key0"); !found || val != 0{
    fmt.Println("Error: did not get expected value after deactivation\n")
  }

  // reactivate node0
  ring.vNodesMux.RLock()
  virtualsCount := len(ring.vNodes)
  ring.vNodesMux.RUnlock()
  ring.Add(nodeId)

  // virtual nodes for node0 should already be in system
  ring.vNodesMux.RLock()
  if virtualsCount != len(ring.vNodes){
    fmt.Println("Error: adding extra virtual nodes after reactivation\n")
  }

  // key0 should be remapped to node0
  ring.vNodesMux.RUnlock()
  if ring.get("key0") != nodeId{
    fmt.Println("Error: node not properly reactivated\n")
  }
  if val, found := ring.Get("key0"); !found || val != 0{
    fmt.Println("Error: did not get expected value after reactivation\n")
  }
}

// test that only necessary keys are remapped after a node crashes
func balancetest() {
  // initialize ring and nodes
  ring := NewRing(1) //one virtual node per real node

  // four real nodes
  var nodes []string
  for i := 0; i < 4; i++ {
    id := "node" + strconv.Itoa(i)
    nodes = append(nodes, id)
    ring.Add(id)
  }

  // four keys
  var wait sync.WaitGroup
  wait.Add(4)
  for i := 0; i < 4; i++ {
    go func(ind int){
      ring.Put("key" + strconv.Itoa(ind), ind)
      wait.Done()
    }(i)
  }

  // count number of keys for each node
  loads := make(map[string]int)
  for _, node := range nodes{
    loads[node] = countLoad(ring, node)
  }

  // deactivate second node
  ring.Remove("node1")
  changed := 0

  // check how many nodes had key mappings changed after node1 crashed
  for _, node := range nodes{
    if node != "node1" && loads[node] != countLoad(ring, node) {
      changed++
    }
  }

  // only one key should have moved
  if changed > 1{
    fmt.Println("Error: more keys moved than needed")
  }
}

// helper function to count number of keys mapped to a node
func countLoad(ring *RingHash, nodeId string) int{
  node, _ := ring.nodes.Get(nodeId)
  count := 0
  for itr := node.(*Node).localMap.Iterator();itr.HasNext(); {
  	count++
    itr.Next()
  }
  return count
}

// simulation of dht with occasional node crashes and recoveries
func demo() {
  // initialize ring and nodes
  ring := NewRing(10) // ten virtual nodes per real node

  // 25 real nodes
  var nodes []string
  var nodesMux = &sync.Mutex{}
  for i := 0; i < 25; i++ {
    id := "node" + strconv.Itoa(i)
    nodes = append(nodes, id)
    ring.Add(id)
  }

  // 50 keys
  var wait sync.WaitGroup
  wait.Add(50)
  for i := 0; i < 50; i++ {
    go func(ind int){
      ring.Put("key" + strconv.Itoa(ind), ind)
      wait.Done()
    }(i)
  }

  wait.Add(50)

  r := rand.New(rand.NewSource(99))
  for i := 0; i < 50; i++{
    if i % 3 == 0{
      // remove or add node
      go func(coinflip int){
        nodesMux.Lock()
        defer nodesMux.Unlock()

        if coinflip % 2 == 0{
          // node (chosen at random) crashes
          // Note: chosen node may have already crashed
          if len(nodes) > 0 {
            ind := 0
            if len(nodes) > 1 {
            ind = r.Intn(len(nodes) - 1)
            }
            //fmt.Printf("removing node%d\n", ind)
            ring.Remove("node" + strconv.Itoa(ind))
          }
        } else {
          // node (chosen at random) recovers or is added to system
          // Note: chosen node may already be functional
          ind := 0
          if len(nodes) > 0 {
            ind = r.Intn(len(nodes))
          }
          //fmt.Printf("adding node%d\n", ind)
          nodeId := "node" + strconv.Itoa(ind)
          ring.Add(nodeId)
          if ind == len(nodes){
            // new node was added to system, add to list
            nodes = append(nodes,nodeId)
          }
        }
        wait.Done()
      }(i)

    } else {
      // get value from key
      go func(k string) {
        v, found := ring.Get(k)
        if !found{
          fmt.Printf("read with key %s into empty entry\n", k)
        } else {
          fmt.Printf("retrieved %d using key %s\n", v, k)
        }
        wait.Done()
      }("key" + strconv.Itoa(i))
    }
  }

  wait.Wait()
}

func main() {
  fmt.Println("Running basic test")
  basictest()
  fmt.Println("\nRunning balance test")
  balancetest()
  fmt.Println("\nRunning demo")
  demo()
}
