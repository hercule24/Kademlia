package kademlia

// Contains the core kademlia type. In addition to core state, this type serves
// as a receiver for the RPC methods, which is required by that package.

import (
	"container/heap"
	//"container/list"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"sync"
	"time"
)

type ContactWrapper struct {
	contact   Contact
	dist      int
	active    bool
	contacted bool
}

//type Channel chan []*ContactWrapper

type ContactHeap []*ContactWrapper

const (
	alpha = 3
	b     = 8 * IDBytes
	k     = 20
)

var short_list *ContactHeap

//var active_num int = 0
var closest_node Contact

var k_mutex = &sync.Mutex{}

// for locking value_map
var v_mutex = &sync.Mutex{}

// for locking short_list
var s_mutex = &sync.Mutex{}

// for locking number of active nodes
//var n_mutex = &sync.Mutex{}

// for locking closesest_node
var c_mutex = &sync.Mutex{}

func (h ContactHeap) Len() int {
	return len(h)
}

func (h ContactHeap) Less(i, j int) bool {
	return h[i].dist < h[j].dist
}

func (h ContactHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *ContactHeap) Push(x interface{}) {
	*h = append(*h, x.(*ContactWrapper))
}

func (h *ContactHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// Kademlia type. You can put whatever state you need in this.
type Kademlia struct {
	NodeID      ID
	SelfContact Contact
	table       *RoutingTable
	value_map   map[ID][]byte
}

func NewKademlia(laddr string) *Kademlia {
	// TODO: Initialize other state here as you add functionality.
	//fmt.Printf("inside NewKademlia")
	k := new(Kademlia)
	k.NodeID = NewRandomID()

	// Set up RPC server
	// NOTE: KademliaCore is just a wrapper around Kademlia. This type includes
	// the RPC functions.
	rpc.Register(&KademliaCore{k})
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", laddr)
	if err != nil {
		log.Fatal("Listen: ", err)
	}
	// Run RPC server forever.
	go http.Serve(l, nil)

	// Add self contact
	hostname, port, _ := net.SplitHostPort(l.Addr().String())
	port_int, _ := strconv.Atoi(port)
	ipAddrStrings, err := net.LookupHost(hostname)
	var host net.IP
	for i := 0; i < len(ipAddrStrings); i++ {
		host = net.ParseIP(ipAddrStrings[i])
		if host.To4() != nil {
			break
		}
	}
	k.SelfContact = Contact{k.NodeID, host, uint16(port_int)}

	k.table = newRoutingTable()

	k.value_map = make(map[ID][]byte)

	return k
}

type NotFoundError struct {
	id  ID
	msg string
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("%x %s", e.id, e.msg)
}

func (k *Kademlia) FindContact(nodeId ID) (*Contact, error) {
	// TODO: Search through contacts, find specified ID
	// Find contact with provided ID
	if nodeId == k.SelfContact.NodeID {
		return &k.SelfContact, nil
	}

	prefix_length := k.NodeID.Xor(nodeId).PrefixLen()
	//fmt.Printf("inside kademlia: prefix_length = %d\n", prefix_length)

	k_mutex.Lock()
	b := k.table.buckets[IDBits-1-prefix_length]
	k_mutex.Unlock()

	for e := b.Front(); e != nil; e = e.Next() {
		if e.Value.(*Contact).NodeID.Equals(nodeId) {
			//fmt.Println("found")
			return e.Value.(*Contact), nil
		}
	}

	return nil, &NotFoundError{nodeId, "Not found"}
}

// This is the function to perform the RPC
func (k *Kademlia) DoPing(host net.IP, port uint16) string {
	// If all goes well, return "OK: <output>", otherwise print "ERR: <messsage>"
	//MsgID := NewRandomID
	ipAddrStrings := host.String()
	sock_addr := ipAddrStrings + ":" + strconv.Itoa(int(port))
	client, err := rpc.DialHTTP("tcp", sock_addr)

	if err != nil {
		return "ERR: can't reach the server"
	}

	pong := new(PongMessage)

	ping := PingMessage{k.SelfContact, NewRandomID()}
	err = client.Call("KademliaCore.Ping", ping, pong)
	if err != nil {
		return "ERR: can't do ping"
	}

	k_mutex.Lock()
	k.Update(&pong.Sender)
	k_mutex.Unlock()
	return "OK: Ping success!"

}

func (k *Kademlia) DoStore(contact *Contact, key ID, value []byte) string {
	// If all goes well, return "OK: <output>", otherwise print "ERR: <messsage>"
	req := StoreRequest{k.SelfContact, NewRandomID(), key, value}

	ipAddrStrings := contact.Host.String()
	port := strconv.Itoa(int(contact.Port))

	sock_addr := ipAddrStrings + ":" + port
	client, err := rpc.DialHTTP("tcp", sock_addr)

	if err != nil {
		return "ERR: can't reach the server"
	}

	res := new(StoreResult)

	err = client.Call("KademliaCore.Store", req, res)

	if err != nil {
		return "ERR: can't do store"
	}

	k_mutex.Lock()
	k.Update(contact)
	k_mutex.Unlock()

	return "OK: store success!"
}

func (k *Kademlia) DoFindNode(contact *Contact, searchKey ID) string {
	// If all goes well, return "OK: <output>", otherwise print "ERR: <messsage>"
	req := FindNodeRequest{k.SelfContact, NewRandomID(), searchKey}
	ipAddrStrings := contact.Host.String()
	port := strconv.Itoa(int(contact.Port))

	sock_addr := ipAddrStrings + ":" + port
	client, err := rpc.DialHTTP("tcp", sock_addr)

	if err != nil {
		return "ERR: can't reach the server"
	}

	res := new(FindNodeResult)

	err = client.Call("KademliaCore.FindNode", req, res)

	if err != nil {
		return "ERR: can't do find node"
	}

	k_mutex.Lock()
	k.Update(contact)
	k_mutex.Unlock()

	for _, v := range res.Nodes {
		fmt.Printf("Node ID: %s\n", v.NodeID.AsString())
	}

	return "OK: FindNode success!"
}

func (k *Kademlia) DoFindValue(contact *Contact, searchKey ID) string {
	// If all goes well, return "OK: <output>", otherwise print "ERR: <messsage>"
	req := FindValueRequest{k.SelfContact, NewRandomID(), searchKey}
	ipAddrStrings := contact.Host.String()
	port := strconv.Itoa(int(contact.Port))

	sock_addr := ipAddrStrings + ":" + port
	client, err := rpc.DialHTTP("tcp", sock_addr)

	if err != nil {
		return "ERR: can't reach the server"
	}

	res := new(FindValueResult)

	err = client.Call("KademliaCore.FindValue", req, res)
	if err != nil {
		return "can't do find value"
	}

	k_mutex.Lock()
	k.Update(contact)
	k_mutex.Unlock()

	if res.Value != nil {
		//fmt.Printf("%v\n", string(res.Value))
		fmt.Printf("From node: %s, found value: %s\n", contact.NodeID.AsString(), string(res.Value))
		return "OK: Value found"
	} else {
		for _, v := range res.Nodes {
			//fmt.Printf("length of res.Nodes = %d", len(res.Nodes))
			fmt.Printf("Node ID: %s\n", v.NodeID.AsString())
		}

		return "ERR: Value not found"
	}

}

func (k *Kademlia) LocalFindValue(searchKey ID) string {
	// If all goes well, return "OK: <output>", otherwise print "ERR: <messsage>"
	v_mutex.Lock()
	value, ok := k.value_map[searchKey]
	v_mutex.Unlock()
	if ok == false {
		return "ERR: can't find value locally"
	} else {
		// shall we include the valude this way?
		return "OK: " + string(value)
	}
}

func (k *Kademlia) sendFindNode(cw *ContactWrapper, id ID, sync_chan chan int) {
	contact := cw.contact
	req := FindNodeRequest{k.SelfContact, NewRandomID(), id}
	ipAddrStrings := contact.Host.String()
	port := strconv.Itoa(int(contact.Port))

	sock_addr := ipAddrStrings + ":" + port
	client, err := rpc.DialHTTP("tcp", sock_addr)

	//short_list = new(ContactHeap)

	cw.contacted = true

	if err != nil {
		// should we return here
		cw.active = false
		//s_mutex.Lock()
		//for i := 0; i < len(*short_list); i++ {
		//	if (*short_list)[i].(*ContactWrapper).contact.NodeID.Equals(cw.contact.NodeID) {
		//	        // It's mentioned in the spec, the node should be removed temporarily,
		//		// but how???
		//		heap.Remove(short_list, i)

		//	}

		//}
		//s_mutex.Unlock()
		return
	}

	res := new(FindNodeResult)

	err = client.Call("KademliaCore.FindNode", req, res)

	if err != nil {
		cw.active = false
		return
		//s_mutex.Lock()
		//for i := 0; i < len(*short_list); i++ {
		//	if (*short_list)[i].(*ContactWrapper).contact.NodeID.Equals(cw.contact.NodeID) {
		//		heap.Remove(short_list, i)

		//	}

		//}
		//s_mutex.Unlock()
	}

	cw.active = true

	for i := 0; i < len(res.Nodes); i++ {
		s_mutex.Lock()

		exist := false
		for j := 0; j < len(*short_list); j++ {
			if res.Nodes[i].NodeID.Equals((*short_list)[j].contact.NodeID) {
				exist = true
			}
		}

		// add only non existent node
		if !exist {

			dist := res.Nodes[i].NodeID.Xor(id).PrefixLen()

			item := new(ContactWrapper)
			item.contact = res.Nodes[i]
			item.dist = dist
			item.contacted = false
			item.active = false
			heap.Push(short_list, item)
		}

		s_mutex.Unlock()
	}

	sync_chan <- 1

}

func (k *Kademlia) DoIterativeFindNode(id ID) []Contact {
	// For project 2!
	//log.Printf("inside the DoIterativeFindNode")
	first_alpha := k.FindKClosest(id, k.NodeID, alpha)
	closest_node = first_alpha[0]

	sync_chan := make(chan int)

	//wrapper_chan := make(Channel)

	//var short_list ContactHeap
	short_list = new(ContactHeap)

	for i := 0; i < len(first_alpha); i++ {
		dist := first_alpha[i].NodeID.Xor(id).PrefixLen()

		item := new(ContactWrapper)
		item.contact = first_alpha[i]
		item.dist = dist
		item.active = false
		item.contacted = false

		*short_list = append(*short_list, item)
	}

	prev_length := len(*short_list)

	heap.Init(short_list)

	for i := 0; i < len(first_alpha); i++ {
		fmt.Println((*short_list)[i].contact.NodeID.AsString())
	}
	fmt.Println("Before the for")

	// stops when there are 20 active,
	// or no contact returned are closer than currently
	// exist in the short list.

	for {
		//var curr_alpha [alpha]*ContactWrapper
		// sends alpha RPCs in one cycle
		count := 0
		s_l := *short_list
		for i := 0; i < len(s_l) && count < alpha; i++ {
			if !s_l[i].contacted {
				go k.sendFindNode(s_l[i], id, sync_chan)
				count++
			}
		}

		time.Sleep(300 * time.Millisecond)

		num_fin := 0

		for {
			select {
			case v := <-sync_chan:
				num_fin += v
				if num_fin == 3 {
					break
				}
			}
		}

		// remove those contacted but not active node
		for i := 0; i < len(*short_list); i++ {
			if (*short_list)[i].contacted && !(*short_list)[i].active {
				heap.Remove(short_list, i)
			}
		}

		for i := 0; i < len(first_alpha); i++ {
			fmt.Println((*short_list)[i].contact.NodeID.AsString())
		}
		fmt.Println("Before the pop")

		temp := (*short_list)[0]

		// larger than prev_length means
		// the short_list has been changed
		if len(*short_list) > prev_length && temp.contact.NodeID.Equals(closest_node.NodeID) {
			var ret []Contact
			for i := 0; i < len(*short_list); i++ {
				ret = append(ret, (*short_list)[i].contact)
			}
			return ret
		} else {
			closest_node = temp.contact
		}

		active_num := 0
		for i := 0; i < len(*short_list); i++ {
			if (*short_list)[i].active {
				active_num++
				if active_num == 20 {
					// return value says K triples returned and converted to string
					var ret []Contact
					for i := 0; i < len(*short_list); i++ {
						ret = append(ret, (*short_list)[i].contact)
					}
					return ret
				}

			}
		}

		prev_length = len(*short_list)
	}

}
func (k *Kademlia) DoIterativeStore(key ID, value []byte) string {
	// For project 2!
	//log.Printf("inside the DoIterativeStore")
	return "ERR: Not implemented"
}
func (k *Kademlia) DoIterativeFindValue(key ID) string {
	// For project 2!
	//log.Printf("inside the DoFindValue")
	return "ERR: Not implemented"
}
