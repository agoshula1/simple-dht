// implementation of a dht with consistent hashing
// alteration and expansion on an implementation in the groupcache repository:
// https://github.com/golang/groupcache/blob/master/consistenthash/consistenthash.go
package main

import (
    "github.com/jabong/florest-core/src/common/collections/maps/concurrentmap/concurrenthashmap"
    "fmt"
    //"io"
    "hash/fnv"
    //"encoding/binary"
    "sync"
    "math/rand"
    "strconv"
    //"time"
)

type Node struct {
  alive bool
  localMap *concurrenthashmap.Map
}

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}

type RingHash struct {
  vNodes []int //virtual nodes
  virtuals int
  nodes map[string]Node* //actual nodes
  hashMap map[int]string //mapping from hash of virtual node to corresponding real node
  db *concurrenthashmap.Map //representing data base server
}

// Returns true if there are no available nodes
func (r *RingHash) IsEmpty() bool {
	for _, node := range r.nodes{
    // inactive nodes may be represented in system
    if node.alive{
      return false
    }
  }
  return true
}

// Adds some nodes to the ring
func (r *RingHash) Add(nodes ...string) {
  for _, node := range nodes {
    // check if node is already present
    if found, existingNode := r.nodes[node]; found{
      if !existingNode.alive{
        // reactivate node
        existingNode.alive = true
        fmt.Printf("%s reactivated: %b", node, r.nodes[node].alive)
      }
    } else {
      // create node
      r.nodes[node] := &Node{Alive: true, LocalMap: concurrenthashmap.New()}

      //create corresponding virtual nodes
      for i := 0; i < r.virtuals; i++ {
  			hash := hash([]byte(strconv.Itoa(i) + node))
  			r.vNodes = append(r.vNodes, hash)
  			r.hashMap[hash] = node
  		}
    }
	}
	sort.Ints(r.vNodes)
}

// Deactivate node in ring
func (r *RingHash) Remove(node string) {
  if found, existingNode := r.nodes[node]; found && existingNode.alive{
    // deactivate node
    existingNode.alive = false
    fmt.Printf("%s deactivated: %b", node, !r.nodes[node].alive)
  }
}

// Gets the closest node in the ring to the provided key
// May point to deactivated node
func (r *RingHash) get(key string) string {
	if r.IsEmpty() {
		return ""
	}

	hash := hash([]byte(key))

	// Binary search for appropriate virtual node.
	idx := sort.Search(len(r.vNodes), func(i int) bool { return r.vNodes[i] >= hash })

	// Means we have cycled back to the first virtual node.
	if idx == len(r.vNodes) {
		idx = 0
	}

  nodeId := r.hashMap[r.vNodes[idx]]
  // find next active node
  for ; r.nodes[nodeId].alive; {
    idx++
    // Means we have cycled back to the first virtual node.
    if idx == len(r.vNodes) {
      idx = 0
    }
    nodeId = r.hashMap[r.vNodes[idx]]
  }

	return nodeId
}

// Get the value mapped to the key
func (r *RingHash) Get(key string) (bool, int) {
  nodeId := r.get(key)

  if len(nodeId) > 0{
    // check if key exists in node
    if found, value := r.nodes[nodeId].localMap.Get(key); !found{
      foundDB, val := r.db.Get(key)
      if !foundDB{
        return false, -1
      }

      // add (key, value) pair to node
      r.nodes[nodeId].localMap.Put(key, val)
      return true, val
    }

    return true, value
  }
}

// Put the (key, value) pair in ring
func (r *RingHash) Put(key string, value int) {
  // add to db server
  r.db.Put(key, value)

  // add to closest node
  nodeId := r.get(key)
  if len(nodeId) > 0{
    r.nodes[nodeId].localMap.Put(key, value)
  }
}
/*
func main() {
  //initialize
  var nodes [8]*Node

  for i := 0; i < 8; i++ {
    nodes[i] = &Node{Alive: true, LocalMap: concurrenthashmap.New()}
  }

  var wait sync.WaitGroup
  wait.Add(100)

  r := rand.New(rand.NewSource(99))
  for i := 0; i < 100; i++{
    if i % 5 == 0{

      //get (key)
      go func(k string) {
        h := sha1.New()
        io.WriteString(h, k)
        n := binary.BigEndian.Uint64(h.Sum(nil)) % 8
        v, found := nodes[n].LocalMap.Get(k)
        if !found{
          fmt.Printf("read with key %s into empty entry\n", k)
        } else {
          fmt.Printf("retrieved %d from %d using key %s\n", v, n, k)
        }
        wait.Done()
      }("key" + strconv.Itoa(r.Intn(i + 3)))

    } else {

      //put (key,value)
      go func(k string, v int) {
        h := sha1.New()
      	io.WriteString(h, k)
        n := binary.BigEndian.Uint64(h.Sum(nil)) % 8
        nodes[n].LocalMap.Put(k, v)
      	fmt.Printf("stored (%s, %d) in %d\n", k, v, n)
        wait.Done()
      }("key" + strconv.Itoa(i), i)

    }
  }

  wait.Wait()
}*/
