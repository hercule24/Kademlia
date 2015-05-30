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

// for locking vdo_map
var vdo_mutex = &sync.Mutex{}

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
	vdo_map     map[ID]VanashingDataObject
}

func NewKademlia(laddr string) *Kademlia {
	// TODO: Initialize other state here as you add functionality.
	//fmt.Printf("inside NewKademlia")
	k := new(Kademlia)
	k.NodeID = NewRandomID()

	// Set up RPC server
	// NOTE: KademliaCore is just a wrapper around Kademlia. This type includes
	// the RPC functions.
	s := rpc.NewServer()
	s.Register(&KademliaCore{k})
	_, port, _ := net.SplitHostPort(laddr)
	s.HandleHTTP(rpc.DefaultRPCPath+port, rpc.DefaultDebugPath+port)
	//rpc.Register(&KademliaCore{k})
	//rpc.HandleHTTP()
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

	k.vdo_map = make(map[ID]VanashingDataObject)
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
	port_str := strconv.Itoa(int(port))
	client, err := rpc.DialHTTPPath("tcp", host.String()+":"+port_str, rpc.DefaultRPCPath+port_str)
	//ipAddrStrings := host.String()
	//sock_addr := ipAddrStrings + ":" + strconv.Itoa(int(port))
	//client, err := rpc.DialHTTP("tcp", sock_addr)

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

//Store VDO locally
func (k *Kademlia) StoreVDO(VdoID ID, vdo VanashingDataObject) string {
	vdo_mutex.Lock()
	k.vdo_map[VdoID] = vdo
	vdo_mutex.Unlock()
	return "OK: Store VDO success"
}

//Do a GETVDO RPC to get VDO
func (k *Kademlia) DoGETVDO(NodeID ID, VdoID ID) string {
	contact, err := k.FindContact(NodeID)
	if err != nil {
		return "ERR: Can't Find NodeID in local contact"
	}
	req := GetVDORequest{k.SelfContact, NewRandomID(), VdoID}
	port_str := strconv.Itoa(int(contact.Port))
	client, err := rpc.DialHTTPPath("tcp", contact.Host.String()+":"+port_str, rpc.DefaultRPCPath+port_str)
	if err != nil {
		return "ERR: can't reach the server"
	}
	res := new(GetVDOResult)

	err = client.Call("KademliaCore.GetVDO", req, res)
	if err != nil {
		return "ERR: can't do GETVDO"
	} else {
		k_mutex.Lock()
		k.Update(contact)
		k_mutex.Unlock()
		vdo := res.VDO
		if vdo.NumberKeys == 0 {
			return "Can't find VDO"
		} else {
			return string(UnvanishData(*k, vdo))
		}
	}
}

func (k *Kademlia) DoStore(contact *Contact, key ID, value []byte) string {
	// If all goes well, return "OK: <output>", otherwise print "ERR: <messsage>"
	req := StoreRequest{k.SelfContact, NewRandomID(), key, value}

	//	ipAddrStrings := contact.Host.String()
	//	port := strconv.Itoa(int(contact.Port))

	//	sock_addr := ipAddrStrings + ":" + port
	//	client, err := rpc.DialHTTP("tcp", sock_addr)
	port_str := strconv.Itoa(int(contact.Port))
	client, err := rpc.DialHTTPPath("tcp", contact.Host.String()+":"+port_str, rpc.DefaultRPCPath+port_str)

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
	//ipAddrStrings := contact.Host.String()
	//port := strconv.Itoa(int(contact.Port))

	//sock_addr := ipAddrStrings + ":" + port
	//client, err := rpc.DialHTTP("tcp", sock_addr)

	port_str := strconv.Itoa(int(contact.Port))
	client, err := rpc.DialHTTPPath("tcp", contact.Host.String()+":"+port_str, rpc.DefaultRPCPath+port_str)
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
	s := ""
	for _, v := range res.Nodes {
		s += v.NodeID.AsString()
	}

	return "OK: FindNode success!" + s
}

func (k *Kademlia) DoFindValue(contact *Contact, searchKey ID) string {
	// If all goes well, return "OK: <output>", otherwise print "ERR: <messsage>"
	req := FindValueRequest{k.SelfContact, NewRandomID(), searchKey}
	//ipAddrStrings := contact.Host.String()
	//port := strconv.Itoa(int(contact.Port))

	//sock_addr := ipAddrStrings + ":" + port
	//client, err := rpc.DialHTTP("tcp", sock_addr)

	port_str := strconv.Itoa(int(contact.Port))
	client, err := rpc.DialHTTPPath("tcp", contact.Host.String()+":"+port_str, rpc.DefaultRPCPath+port_str)
	if err != nil {
		return "ERR: can't reach the server"
	}

	res := new(FindValueResult)

	err = client.Call("KademliaCore.FindValue", req, res)
	if err != nil {
		return "ERR: can't do find value"
	}

	k_mutex.Lock()
	k.Update(contact)
	k_mutex.Unlock()

	if res.Value != nil {
		//fmt.Printf("%v\n", string(res.Value))
		//fmt.Printf("From node: %s, found value: %s\n", contact.NodeID.AsString(), string(res.Value))
		return "OK: Value found" + string(res.Value)
	} else {
		s := ""
		for _, v := range res.Nodes {
			//fmt.Printf("length of res.Nodes = %d", len(res.Nodes))
			s += v.NodeID.AsString()
		}

		return "OK: Nodes returned." + s
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
		return "OK: " + string(value)
	}
}

func (k *Kademlia) sendFindNode(cw *ContactWrapper, id ID, sync_chan chan int) {
	//senf FindNode RPCs
	contact := cw.contact
	req := FindNodeRequest{k.SelfContact, NewRandomID(), id}
	//ipAddrStrings := contact.Host.String()
	//port := strconv.Itoa(int(contact.Port))

	//sock_addr := ipAddrStrings + ":" + port
	//client, err := rpc.DialHTTP("tcp", sock_addr)

	port_str := strconv.Itoa(int(contact.Port))
	client, err := rpc.DialHTTPPath("tcp", contact.Host.String()+":"+port_str, rpc.DefaultRPCPath+port_str)
	cw.contacted = true

	if err != nil {
		cw.active = false
		return
	}

	res := new(FindNodeResult)

	err = client.Call("KademliaCore.FindNode", req, res)

	if err != nil {
		cw.active = false
		return
	}

	cw.active = true

	k_mutex.Lock()
	k.Update(&contact)
	k_mutex.Unlock()

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

			dist := IDBits - res.Nodes[i].NodeID.Xor(id).PrefixLen()

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
	// Return k triples of contacts closest to the id
	first_alpha := k.FindKClosest(id, k.NodeID, alpha)
	closest_node = first_alpha[0]

	sync_chan := make(chan int)

	short_list = new(ContactHeap)

	for i := 0; i < len(first_alpha); i++ {
		dist := IDBits - first_alpha[i].NodeID.Xor(id).PrefixLen()

		item := new(ContactWrapper)
		item.contact = first_alpha[i]
		item.dist = dist
		item.active = false
		item.contacted = false

		*short_list = append(*short_list, item)
	}

	heap.Init(short_list)
	// stops when there are 20 active,
	// or no contact returned are closer than currently
	// exist in the short list.

	for {
		// sends alpha RPCs in one cycle
		count := 0
		s_l := *short_list

		for i := 0; i < len(s_l) && count < alpha; i++ {
			if !(s_l)[i].contacted {
				go k.sendFindNode(s_l[i], id, sync_chan)
				count++
			}
		}

		time.Sleep(300 * time.Millisecond)

		num_fin := 0

		for {
			if num_fin == count {
				break
			}
			select {
			case v := <-sync_chan:
				num_fin += v
			}

		}

		// remove those contacted but not active node
		for i := 0; i < len(*short_list); i++ {
			if (*short_list)[i].contacted && !(*short_list)[i].active {
				heap.Remove(short_list, i)
			}
		}

		temp := (*short_list)[0]
		if temp.contact.NodeID.Equals(closest_node.NodeID) {
			active_num := 0
			var ret []Contact
			for i := 0; i < len(*short_list); i++ {
				if !(*short_list)[i].contacted {
					//				ipAddrStrings := (*short_list)[i].contact.Host.String()
					//				port := strconv.Itoa(int((*short_list)[i].contact.Port))
					port_str := strconv.Itoa(int((*short_list)[i].contact.Port))
					_, err := rpc.DialHTTPPath("tcp", (*short_list)[i].contact.Host.String()+":"+port_str, rpc.DefaultRPCPath+port_str)

					//				sock_addr := ipAddrStrings + ":" + port
					//				_, err := rpc.DialHTTP("tcp", sock_addr)
					if err != nil {
						(*short_list)[i].active = false
					} else {
						k_mutex.Lock()
						k.Update(&(*short_list)[i].contact)
						k_mutex.Unlock()
						active_num++
						if active_num <= 20 {
							ret = append(ret, (*short_list)[i].contact)
						} else {
							break
						}
					}
				} else {
					active_num++
					if active_num <= 20 {
						ret = append(ret, (*short_list)[i].contact)
					} else {
						break
					}

				}
			}
			return ret
		} else {
			closest_node = temp.contact
			active_num := 0
			var ret []Contact
			for i := 0; i < len(*short_list); i++ {
				if (*short_list)[i].active {
					active_num++

					if active_num <= 20 {
						ret = append(ret, (*short_list)[i].contact)
					} else {
						break
					}
				}
			}
			if active_num == 20 {
				return ret
			}
		}

	}
}

func (k *Kademlia) DoIterativeStore(key ID, value []byte) string {
	// Store key-value pairs in nodes returned from iterativeFindNode
	closeNodes := k.DoIterativeFindNode(key)
	for _, node := range closeNodes {
		k.DoStore(&node, key, value)
	}

	lastID := closeNodes[len(closeNodes)-1].NodeID
	return lastID.AsString()
}

func (k *Kademlia) sendFindValue(cw *ContactWrapper, key ID, sync_chan chan int, flag_chan chan Contact, val_chan chan []byte) {
	contact := cw.contact
	req := FindValueRequest{k.SelfContact, NewRandomID(), key}
	//ipAddrStrings := contact.Host.String()
	//port := strconv.Itoa(int(contact.Port))

	//sock_addr := ipAddrStrings + ":" + port
	//client, err := rpc.DialHTTP("tcp", sock_addr)

	port_str := strconv.Itoa(int(contact.Port))
	client, err := rpc.DialHTTPPath("tcp", contact.Host.String()+":"+port_str, rpc.DefaultRPCPath+port_str)
	cw.contacted = true

	if err != nil {
		cw.active = false
		return
	}

	res := new(FindValueResult)

	err = client.Call("KademliaCore.FindValue", req, res)

	if err != nil {
		cw.active = false
		return
	}

	cw.active = true

	k_mutex.Lock()
	k.Update(&contact)
	k_mutex.Unlock()

	if res.Value != nil {
		flag_chan <- contact
		val_chan <- res.Value
		closest_node := (*short_list)[0].contact
		s_mutex.Lock()
		if closest_node.NodeID == contact.NodeID {
			closest_node = (*short_list)[1].contact
		}
		s_mutex.Unlock()
		k.DoStore(&closest_node, key, res.Value)
		return
	}

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

			dist := IDBits - res.Nodes[i].NodeID.Xor(key).PrefixLen()

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

func (k *Kademlia) DoIterativeFindValue(key ID) string {
	// Return value if the key is found
	first_alpha := k.FindKClosest(key, k.NodeID, alpha)
	closest_node = first_alpha[0]
	//find value locally
	val, ok := k.value_map[key]
	if ok {
		//if find value locally, store the val at the closest_node
		k.DoStore(&closest_node, key, val)
		s := "From node: " + k.NodeID.AsString() + ", found value: " + string(val)
		return s
	}

	sync_chan := make(chan int)
	//if flag_chan is not empty, then we find the value
	flag_chan := make(chan Contact)
	val_chan := make(chan []byte)
	short_list = new(ContactHeap)
	for i := 0; i < len(first_alpha); i++ {
		dist := IDBits - first_alpha[i].NodeID.Xor(key).PrefixLen()

		item := new(ContactWrapper)
		item.contact = first_alpha[i]
		item.dist = dist
		item.active = false
		item.contacted = false

		*short_list = append(*short_list, item)
	}

	heap.Init(short_list)

	for {
		// sends alpha RPCs in one cycle
		count := 0
		s_l := *short_list

		for i := 0; i < len(s_l) && count < alpha; i++ {
			if !s_l[i].contacted {
				go k.sendFindValue(s_l[i], key, sync_chan, flag_chan, val_chan)
				count++
			}
		}
		//if there is no un-contacted node to look for, return ERR
		if count == 0 {
			return "ERR"
		}

		time.Sleep(300 * time.Millisecond)
		num_fin := 0
		for {
			select {
			case c := <-flag_chan:
				s := "From node: " + c.NodeID.AsString() + ", found value: " + string(<-val_chan)
				return s
			default:
				select {
				case v := <-sync_chan:
					num_fin += v
				}
			}

			if num_fin == count {
				break
			}

		}

		// remove those contacted but not active node
		for i := 0; i < len(*short_list); i++ {
			if (*short_list)[i].contacted && !(*short_list)[i].active {
				heap.Remove(short_list, i)
			}
		}

		temp := (*short_list)[0]
		if temp.contact.NodeID.Equals(closest_node.NodeID) {
			active_num := 0
			var ret []Contact
			for i := 0; i < len(*short_list); i++ {
				if !(*short_list)[i].contacted {
					//ipAddrStrings := (*short_list)[i].contact.Host.String()
					//port := strconv.Itoa(int((*short_list)[i].contact.Port))

					port_str := strconv.Itoa(int((*short_list)[i].contact.Port))
					_, err := rpc.DialHTTPPath("tcp", (*short_list)[i].contact.Host.String()+":"+port_str, rpc.DefaultRPCPath+port_str)
					//sock_addr := ipAddrStrings + ":" + port
					//_, err := rpc.DialHTTP("tcp", sock_addr)
					if err != nil {
						(*short_list)[i].active = false
					} else {
						k_mutex.Lock()
						k.Update(&(*short_list)[i].contact)
						k_mutex.Unlock()
						active_num++
						if active_num <= 20 {
							ret = append(ret, (*short_list)[i].contact)
						} else {
							break
						}
					}
				} else {
					active_num++
					if active_num <= 20 {
						ret = append(ret, (*short_list)[i].contact)
					} else {
						break
					}

				}
			}
			return "ERR"
		} else {
			closest_node = temp.contact
			active_num := 0
			var ret []Contact
			for i := 0; i < len(*short_list); i++ {
				if (*short_list)[i].active {
					active_num++

					if active_num <= 20 {
						ret = append(ret, (*short_list)[i].contact)
					} else {
						break
					}
				}
			}
			if active_num == 20 {
				return "ERR"
			}
		}
	}
}
