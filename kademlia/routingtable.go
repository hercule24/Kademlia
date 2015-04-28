package kademlia

import (
	"container/list"
	//"fmt"
	"sort"
	"strings"
)

const bucket_size = 20

type RoutingTable struct {
	node    Contact
	buckets [IDBits]*list.List
	k       *Kademlia
}

func newRoutingTable(node *Contact, k *Kademlia) (ret *RoutingTable) {
	ret = new(RoutingTable)
	for i := 0; i < 8*IDBytes; i++ {
		ret.buckets[i] = list.New()
	}

	ret.node = *node
	ret.k = k
	return
}

type Contacts []Contact

func (a Contacts) Len() int {
	return len(a)
}

func (a Contacts) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a Contacts) Less(i, j int) bool {
	return a[i].NodeID.Less(a[j].NodeID)
}

func (k *Kademlia) Update(contact *Contact) {
	if contact.NodeID.Equals(k.NodeID) {
		//fmt.Println("Self!")
		return
	}

	prefix_length := contact.NodeID.Xor(k.NodeID).PrefixLen()
	//fmt.Printf("inside RoutingTable: prefix_length = %d\n", prefix_length)
	bucket := k.table.buckets[IDBits-1-prefix_length]
	_, ok := k.FindContact(contact.NodeID)

	// doesn't exist
	if ok != nil {
		if bucket.Len() < bucket_size {
			//fmt.Println("inside update: doesn't exist")
			bucket.PushBack(contact)
		} else {
			//TODO:
			// ping the least recently contacted node, head of the bucket
			e := bucket.Front()
			c := e.Value.(Contact)
			msg := k.DoPing(c.Host, c.Port)

			if strings.HasPrefix(msg, "OK") {
				// ignore, may not fullfill the protocol
				bucket.MoveAfter(bucket.Front(), bucket.Front().Next())
			} else {
				// update least recently seen
				bucket.Remove(bucket.Front())
				bucket.PushBack(contact)
			}

		}
	} else {
		for e := bucket.Front(); e != nil; e = e.Next() {
			if e.Value.(*Contact).NodeID.Equals(contact.NodeID) {
				bucket.MoveToBack(e)
			}
		}
	}

}

func (table *RoutingTable) FindKClosest(target ID) []Contact {
	var ret []Contact
	index := target.Xor(table.node.NodeID).PrefixLen()
	bucket := table.buckets[IDBits-1-index]

	for e := bucket.Front(); e != nil; e = e.Next() {
		ret = append(ret, *(e.Value.(*Contact)))
	}

	count := len(ret)

	if count < bucket_size {
		for i := 0; i < IDBits; i++ {
			if i == index {
				continue
			} else {
				b := table.buckets[i]
				for e := b.Front(); e != nil; e = e.Next() {
					ret = append(ret, *(e.Value.(Contact)))
					count++
				}
			}
		}
	}

	if count < 20 {
		for i := index + 1; i < IDBits; i++ {
			b := table.buckets[i]
			for e := b.Front(); e != nil; e = e.Next() {
				ret = append(ret, e.Value.(Contact))
				count++
			}
		}
	}

	sort.Sort(Contacts(ret))
	return ret[0:21]

}
