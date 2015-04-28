package kademlia

// Contains the core kademlia type. In addition to core state, this type serves
// as a receiver for the RPC methods, which is required by that package.

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
)

const (
	alpha = 3
	b     = 8 * IDBytes
	k     = 20
)

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

	k.table = newRoutingTable(&k.SelfContact, k)

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
	b := k.table.buckets[IDBits-1-prefix_length]

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

	k.Update(&pong.Sender)
	msg := "OK: Ping success!"
	return msg

}

func (k *Kademlia) DoStore(contact *Contact, key ID, value []byte) string {
	// If all goes well, return "OK: <output>", otherwise print "ERR: <messsage>"
	req := StoreRequest{*contact, NewRandomID(), key, value}

	ipAddrStrings := contact.Host.String()
	port := strconv.Itoa(int(contact.Port))

	sock_addr := ipAddrStrings + ":" + port
	client, err := rpc.DialHTTP("tcp", sock_addr)

	if err != nil {
		return "ERR: can't reach the server"
	}

	var res StoreResult

	err = client.Call("KademliaCore.Store", req, &res)

	if err != nil {
		return "ERR: can't do store"
	}

	msg := "OK: StoreResult: " + res.MsgID.AsString()
	return msg
}

func (k *Kademlia) DoFindNode(contact *Contact, searchKey ID) string {
	// If all goes well, return "OK: <output>", otherwise print "ERR: <messsage>"
	req := FindNodeRequest{*contact, NewRandomID(), searchKey}
	ipAddrStrings := contact.Host.String()
	port := strconv.Itoa(int(contact.Port))

	sock_addr := ipAddrStrings + ":" + port
	client, err := rpc.DialHTTP("tcp", sock_addr)

	if err != nil {
		return "ERR: can't reach the server"
	}

	var res FindNodeResult

	err = client.Call("KademliaCore.FindNode", req, &res)

	if err != nil {
		return "can't do find node"
	}

	// nodes find not used, why?
	msg := "OK: FindNode: " + res.MsgID.AsString()
	return msg
}

func (k *Kademlia) DoFindValue(contact *Contact, searchKey ID) string {
	// If all goes well, return "OK: <output>", otherwise print "ERR: <messsage>"
	req := FindValueRequest{*contact, NewRandomID(), searchKey}
	ipAddrStrings := contact.Host.String()
	port := strconv.Itoa(int(contact.Port))

	sock_addr := ipAddrStrings + ":" + port
	client, err := rpc.DialHTTP("tcp", sock_addr)

	if err != nil {
		return "ERR: can't reach the server"
	}

	var res FindValueResult

	err = client.Call("KademliaCore.FindValue", req, &res)
	if err != nil {
		return "can't do find value"
	}

	// values find not used, why?
	msg := "OK: FindValue: " + res.MsgID.AsString()
	return msg

}

func (k *Kademlia) LocalFindValue(searchKey ID) string {
	// If all goes well, return "OK: <output>", otherwise print "ERR: <messsage>"
	value, ok := k.value_map[searchKey]
	if ok == false {
		return "ERR: can't find value locally"
	} else {
		// shall we include the valude this way?
		msg := "OK: " + string(value)
		return msg
	}
}

func (k *Kademlia) DoIterativeFindNode(id ID) string {
	// For project 2!
	//log.Printf("inside the DoIterativeFindNode")
	return "ERR: Not implemented"
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
