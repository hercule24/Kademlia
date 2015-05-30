package kademlia

// Contains definitions mirroring the Kademlia spec. You will need to stick
// strictly to these to be compatible with the reference implementation and
// other groups' code.

import (
	"net"
)

type KademliaCore struct {
	kademlia *Kademlia
}

// Host identification.
type Contact struct {
	NodeID ID
	Host   net.IP
	Port   uint16
}

///////////////////////////////////////////////////////////////////////////////
// PING
///////////////////////////////////////////////////////////////////////////////
type PingMessage struct {
	Sender Contact
	MsgID  ID
}

type PongMessage struct {
	MsgID  ID
	Sender Contact
}

func (kc *KademliaCore) Ping(ping PingMessage, pong *PongMessage) error {
	pong.MsgID = CopyID(ping.MsgID)
	// Specify the sender
	pong.Sender = kc.kademlia.SelfContact
	// Update contact, etc

	k_mutex.Lock()
	kc.kademlia.Update(&ping.Sender)
	k_mutex.Unlock()
	return nil
}

///////////////////////////////////////////////////////////////////////////////
// STORE
///////////////////////////////////////////////////////////////////////////////
type StoreRequest struct {
	Sender Contact
	MsgID  ID
	Key    ID
	Value  []byte
}

type StoreResult struct {
	MsgID ID
	Err   error
}

func (kc *KademliaCore) Store(req StoreRequest, res *StoreResult) error {
	v_mutex.Lock()
	kc.kademlia.value_map[req.Key] = req.Value
	v_mutex.Unlock()

	res.MsgID = CopyID(req.MsgID)

	k_mutex.Lock()
	kc.kademlia.Update(&req.Sender)
	k_mutex.Unlock()

	res.Err = nil

	// should we consider the data length problem stated in the spec?
	return nil
}

///////////////////////////////////////////////////////////////////////////////
// FIND_NODE
///////////////////////////////////////////////////////////////////////////////
type FindNodeRequest struct {
	Sender Contact
	MsgID  ID
	NodeID ID
}

type FindNodeResult struct {
	MsgID ID
	Nodes []Contact
	Err   error
}

func (kc *KademliaCore) FindNode(req FindNodeRequest, res *FindNodeResult) error {
	k_mutex.Lock()
	kc.kademlia.Update(&req.Sender)
	k_mutex.Unlock()

	node := req.NodeID

	// copy?
	res.MsgID = CopyID(req.MsgID)

	k_mutex.Lock()
	res.Nodes = kc.kademlia.FindKClosest(node, req.Sender.NodeID, 20)
	k_mutex.Unlock()

	// always nil ??
	res.Err = nil

	return nil
}

///////////////////////////////////////////////////////////////////////////////
// FIND_VALUE
///////////////////////////////////////////////////////////////////////////////
type FindValueRequest struct {
	Sender Contact
	MsgID  ID
	Key    ID
}

// If Value is nil, it should be ignored, and Nodes means the same as in a
// FindNodeResult.
type FindValueResult struct {
	MsgID ID
	Value []byte
	Nodes []Contact
	Err   error
}

func (kc *KademliaCore) FindValue(req FindValueRequest, res *FindValueResult) error {
	k_mutex.Lock()
	kc.kademlia.Update(&req.Sender)
	k_mutex.Unlock()

	key := req.Key

	res.MsgID = CopyID(req.MsgID)
	res.Err = nil

	v_mutex.Lock()
	value, ok := kc.kademlia.value_map[key]
	v_mutex.Unlock()

	// not found
	if ok == false {
		k_mutex.Lock()
		res.Nodes = kc.kademlia.FindKClosest(key, req.Sender.NodeID, 20)
		k_mutex.Unlock()
		res.Value = nil
	} else {
		res.Nodes = nil
		res.Value = value
	}

	return nil
}

///////////////////////////////////////////////////////////////////////////////
// GET_VDO
///////////////////////////////////////////////////////////////////////////////
type GetVDORequest struct {
	Sender Contact
	MsgID  ID
	VdoID  ID
}

type GetVDOResult struct {
	MsgID ID
	VDO   VanashingDataObject
}

func (kc *KademliaCore) GetVDO(req GetVDORequest, res *GetVDOResult) error {
	k_mutex.Lock()
	kc.kademlia.Update(&req.Sender)
	k_mutex.Unlock()

	res.MsgID = CopyID(req.MsgID)

	vdo_mutex.Lock()
	vdo, ok := kc.kademlia.vdo_map[req.VdoID]
	vdo_mutex.Unlock()
	if ok == true {
		res.VDO = vdo
	}

	return nil
}
