package kademlia

import (
	"container/list"
	//"fmt"
	"log"
	"net/rpc"
	"sort"
	"strconv"
	//"strings"
)

const bucket_size = 20

type RoutingTable struct {
	buckets [IDBits]*list.List
}

func newRoutingTable() (ret *RoutingTable) {
	ret = new(RoutingTable)
	for i := 0; i < 8*IDBytes; i++ {
		ret.buckets[i] = list.New()
	}

	return
}

type Contact_Dist struct {
	contact Contact
	dist    int
}

type Contact_Dist_Slice []Contact_Dist

func (a Contact_Dist_Slice) Len() int {
	return len(a)
}

func (a Contact_Dist_Slice) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a Contact_Dist_Slice) Less(i, j int) bool {
	return a[i].dist < a[j].dist
}

func (k *Kademlia) Update(contact *Contact) {
	if contact.NodeID.Equals(k.NodeID) {
		//fmt.Println("Self!")
		return
	}

	prefix_length := contact.NodeID.Xor(k.NodeID).PrefixLen()
	//fmt.Printf("inside RoutingTable: prefix_length = %d\n", prefix_length)

	bucket := k.table.buckets[IDBits-1-prefix_length]

	// if found
	for e := bucket.Front(); e != nil; e = e.Next() {
		if e.Value.(*Contact).NodeID.Equals(contact.NodeID) {
			bucket.MoveToBack(e)
			return
		}
	}

	// if not found
	if bucket.Len() < bucket_size {
		//fmt.Println("inside update: doesn't exist")
		bucket.PushBack(contact)
	} else {
		//TODO:
		// ping the least recently contacted node, head of the bucket
		e := bucket.Front()
		c := e.Value.(Contact)

		ipAddrStrings := c.Host.String()
		sock_addr := ipAddrStrings + ":" + strconv.Itoa(int(c.Port))
		client, err := rpc.DialHTTP("tcp", sock_addr)

		if err != nil {
			log.Fatal("ERR: ", err)
		}

		pong := new(PongMessage)

		ping := PingMessage{k.SelfContact, NewRandomID()}
		err = client.Call("KademliaCore.Ping", ping, pong)

		if err == nil {
			// ignore, may not fullfill the protocol
			bucket.MoveToBack(e)
		} else {
			// update least recently seen
			bucket.Remove(bucket.Front())
			bucket.PushBack(contact)
		}

	}

}

func (k *Kademlia) FindKClosest(target ID, requestor ID, alpha int) []Contact {
	var ret []Contact
	var temp []Contact_Dist

	// if self
	if target.Equals(k.NodeID) {
		s := Contact_Dist{k.SelfContact, 0}
		temp = append(temp, s)

		for i := 0; i < IDBits; i++ {
			b := k.table.buckets[i]
			for e := b.Front(); e != nil; e = e.Next() {
				c := *(e.Value.(*Contact))
				if c.NodeID.Equals(requestor) {
					continue
				}
				dist := IDBits - c.NodeID.Xor(target).PrefixLen()
				s := Contact_Dist{c, dist}
				temp = append(temp, s)
			}

			if len(temp) >= alpha {
				break
			}
		}

	} else {
		prefix_length := target.Xor(k.NodeID).PrefixLen()
		bucket_index := IDBits - 1 - prefix_length

		bucket := k.table.buckets[bucket_index]

		for e := bucket.Front(); e != nil; e = e.Next() {
			c := *(e.Value.(*Contact))
			if c.NodeID.Equals(requestor) {
				continue
			}
			dist := IDBits - c.NodeID.Xor(target).PrefixLen()
			s := Contact_Dist{c, dist}
			temp = append(temp, s)
		}

		if len(temp) < alpha {
			for i := 0; i < bucket_index; i++ {
				b := k.table.buckets[i]
				for e := b.Front(); e != nil; e = e.Next() {
					c := *(e.Value.(*Contact))
					if c.NodeID.Equals(requestor) {
						continue
					}
					dist := IDBits - c.NodeID.Xor(target).PrefixLen()
					s := Contact_Dist{c, dist}
					temp = append(temp, s)
				}
			}
		}

		if len(temp) < alpha {
			for i := bucket_index + 1; i < IDBits; i++ {
				b := k.table.buckets[i]
				for e := b.Front(); e != nil; e = e.Next() {
					c := *(e.Value.(*Contact))
					if c.NodeID.Equals(requestor) {
						continue
					}
					dist := IDBits - c.NodeID.Xor(target).PrefixLen()
					s := Contact_Dist{c, dist}
					temp = append(temp, s)
				}

				if len(temp) >= alpha {
					break
				}

			}
		}
	}

	sort.Sort(Contact_Dist_Slice(temp))

	if len(temp) < alpha {
		for i := 0; i < len(temp); i++ {
			ret = append(ret, temp[i].contact)
		}
	} else {
		for i := 0; i < alpha; i++ {
			ret = append(ret, temp[i].contact)
		}

	}

	return ret
}
