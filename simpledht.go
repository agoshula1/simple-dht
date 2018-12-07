package main

import (
    "github.com/jabong/florest-core/src/common/collections/maps/concurrentmap/concurrenthashmap"
    "fmt"
    "io"
    "crypto/sha1"
    "encoding/binary"
    "sync"
    "math/rand"
    "strconv"
    //"time"
)

type Node struct {
  Alive bool
  LocalMap *concurrenthashmap.Map
}

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
}
