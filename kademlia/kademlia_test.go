package kademlia

import (
	"net"
	"strconv"
	"strings"
	"testing"
)

//Equality between two []byte, expecially for net.IP
func SliceEq(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
func StringToIpPort(laddr string) (ip net.IP, port uint16, err error) {
	hostString, portString, err := net.SplitHostPort(laddr)
	if err != nil {
		return
	}
	ipStr, err := net.LookupHost(hostString)
	if err != nil {
		return
	}
	for i := 0; i < len(ipStr); i++ {
		ip = net.ParseIP(ipStr[i])
		if ip.To4() != nil {
			break
		}
	}
	portInt, err := strconv.Atoi(portString)
	port = uint16(portInt)
	return
}

var instance1 = NewKademlia("localhost:7890")
var instance2 = NewKademlia("localhost:7891")
var instance3 = NewKademlia("localhost:7892")
var instance4 = NewKademlia("localhost:7893")
var instance5 = NewKademlia("localhost:7894")
var instance6 = NewKademlia("localhost:7895")
var instance7 = NewKademlia("localhost:7896")
var host1, port1, _ = StringToIpPort("localhost:7890")
var host2, port2, _ = StringToIpPort("localhost:7891")
var host3, port3, _ = StringToIpPort("localhost:7892")
var host4, port4, _ = StringToIpPort("localhost:7893")
var host5, port5, _ = StringToIpPort("localhost:7894")
var host6, port6, _ = StringToIpPort("localhost:7895")
var host7, port7, _ = StringToIpPort("localhost:7896")

func TestPing(t *testing.T) {
	instance1.DoPing(host2, port2)
	contact2, err := instance1.FindContact(instance2.NodeID)
	if err != nil {
		t.Error("Instance 2's contact not found in Instance 1's contact list")
		return
	}
	contact1, err := instance2.FindContact(instance1.NodeID)
	if err != nil {
		t.Error("Instance 1's contact not found in Instance 2's contact list")
		return
	}
	if contact1.NodeID != instance1.NodeID {
		t.Error("Instance 1 ID incorrectly stored in Instance 2's contact list")
		return
	}
	if !SliceEq(contact1.Host, instance1.SelfContact.Host) {
		t.Error("Instance 1 IP incorrectly stored in Instance 2's contact list")
		return
	}
	if contact1.Port != instance1.SelfContact.Port {
		t.Error("Instance 1 Port incorrectly stored in Instance 2's contact list")
		return
	}
	if contact2.NodeID != instance2.NodeID {
		t.Error("Instance 2 ID incorrectly stored in Instance 1's contact list")
		return
	}
	if !SliceEq(contact2.Host, instance2.SelfContact.Host) {
		t.Error("Instance 2 IP incorrectly stored in Instance 1's contact list")
		return
	}
	if contact2.Port != instance2.SelfContact.Port {
		t.Error("Instance 2 Port incorrectly stored in Instance 1's contact list")
		return
	}
}

func TestStore(t *testing.T) {
	key := NewRandomID()
	value := []byte("hello")
	if !strings.Contains(instance1.DoStore(&instance2.SelfContact, key, value), "OK") {
		t.Error("unexpected store failure")
		return
	}
	if !strings.Contains(instance2.LocalFindValue(key), "hello") {
		t.Error("Instance 2 failed to store value locally")
		return
	}
}
func TestFindNode(t *testing.T) {
	instance2.DoPing(host3, port3)
	instance2.DoPing(host4, port4)
	instance3.DoPing(host5, port5)
	instance3.DoPing(host6, port6)
	instance4.DoPing(host7, port7)
	//instance1 send findnode rpc to instance2 looking for instance3
	s := instance1.DoFindNode(&instance2.SelfContact, instance3.NodeID)
	if !strings.Contains(s, "OK") {
		t.Error("unexpected findnode failure")
		return
	}
	if !strings.Contains(s, instance3.NodeID.AsString()) {
		t.Error("Instance 3's contact not found in the findnode result")
		return
	}
	if !strings.Contains(s, instance4.NodeID.AsString()) {
		t.Error("Instance 4's contact not found in the findnode result")
		return
	}
	if strings.Contains(s, instance1.NodeID.AsString()) {
		t.Error("Instance 1 unexpectely returned in the findnode result")
		return
	}
	//instance2 send findnode rpc to instance3 looking for instance3
	s1 := instance2.DoFindNode(&instance3.SelfContact, instance3.NodeID)
	if !strings.Contains(s1, "OK") {
		t.Error("unexpected findnode failure")
		return
	}
	if !strings.Contains(s1, instance3.NodeID.AsString()) {
		t.Error("Instance 3's contact not found in the findnode result")
		return
	}
	if !strings.Contains(s1, instance5.NodeID.AsString()) {
		t.Error("Instance 5's contact not found in the findnode result")
		return
	}
	if !strings.Contains(s1, instance6.NodeID.AsString()) {
		t.Error("Instance 6's contact not found in the findnode result")
		return
	}
	if strings.Contains(s1, instance2.NodeID.AsString()) {
		t.Error("Instance 2 unexpectely found in the findnode result")
		return
	}

}
func TestFindValue(t *testing.T) {
	randomKey := NewRandomID()
	//when there is no matching key, return nodes
	s := instance1.DoFindValue(&instance2.SelfContact, randomKey)
	if !strings.Contains(s, "OK") {
		t.Error("unexpected findvalue failure")
		return
	}
	if !strings.Contains(s, instance3.NodeID.AsString()) {
		t.Error("Instance 3's contact not found in the findvalue result")
		return
	}
	if !strings.Contains(s, instance4.NodeID.AsString()) {
		t.Error("Instance 4's contact not found in the findvalue result")
		return
	}
	//when there is matching key, return value
	key := NewRandomID()
	value := []byte("world")
	if !strings.Contains(instance3.DoStore(&instance2.SelfContact, key, value), "OK") {
		t.Error("unexpected store failure")
		return
	}
	if !strings.Contains(instance2.LocalFindValue(key), "world") {
		t.Error("Instance 2 failed to store value locally")
		return
	}
	s1 := instance1.DoFindValue(&instance2.SelfContact, key)
	if !strings.Contains(s1, "world") {
		t.Error("can't find value")
		return
	}

}

func TestDoIterativeFindNode(t *testing.T) {
	//search for the node with 1 hop away
	s := ""
	for i, v := range instance1.DoIterativeFindNode(instance4.NodeID) {
		if i == 0 && v.NodeID != instance4.NodeID {
			t.Error("the closest node is not correct")
			return
		}
		s += v.NodeID.AsString() + "\n"
	}
	if !strings.Contains(s, instance2.NodeID.AsString()) {
		t.Error("Instance 2 is not returned from DoIterativeFindNode")
		return
	}
	if !strings.Contains(s, instance3.NodeID.AsString()) {
		t.Error("Instance 3 is not returned from DoIterativeFindNode")
		return
	}
	if !strings.Contains(s, instance4.NodeID.AsString()) {
		t.Error("Instance 4 is not returned from DoIterativeFindNode")
		return
	}
	if !strings.Contains(s, instance5.NodeID.AsString()) {
		t.Error("Instance 5 is not returned from DoIterativeFindNode")
		return
	}
	if !strings.Contains(s, instance6.NodeID.AsString()) {
		t.Error("Instance 6 is not returned from DoIterativeFindNode")
		return
	}
	if !strings.Contains(s, instance7.NodeID.AsString()) {
		t.Error("Instance 7 is not returned from DoIterativeFindNode")
		return
	}
	//search for the node with 2 hops away, stops because closest node is not updated
	instance8 := NewKademlia("localhost:7897")
	instance8.DoPing(host2, port2)
	s1 := ""
	for _, v := range instance8.DoIterativeFindNode(instance5.NodeID) {
		s1 += v.NodeID.AsString() + "\n"
	}
	if !strings.Contains(s1, instance1.NodeID.AsString()) {
		t.Error("Instance 1 is not returned from DoIterativeFindNode")
		return
	}
	if !strings.Contains(s1, instance2.NodeID.AsString()) {
		t.Error("Instance 2 is not returned from DoIterativeFindNode")
		return
	}
	if !strings.Contains(s1, instance3.NodeID.AsString()) {
		t.Error("Instance 3 is not returned from DoIterativeFindNode")
		return
	}
	if !strings.Contains(s1, instance4.NodeID.AsString()) {
		t.Error("Instance 4 is not returned from DoIterativeFindNode")
		return
	}
}
func TestDoIterativeStore(t *testing.T) {
	//instance7 store a key same as instance2's ID with value "2"
	key := instance2.NodeID
	instance7.DoIterativeStore(key, []byte("2"))
	if !strings.Contains(instance1.LocalFindValue(key), "2") {
		t.Error("Instance 1 didn't store the proper value")
		return
	}
	if !strings.Contains(instance2.LocalFindValue(key), "2") {
		t.Error("Instance 2 didn't store the proper value")
		return
	}
	if !strings.Contains(instance3.LocalFindValue(key), "2") {
		t.Error("Instance 3 didn't store the proper value")
		return
	}
	if !strings.Contains(instance4.LocalFindValue(key), "2") {
		t.Error("Instance 4 didn't store the proper value")
		return
	}
	if !strings.Contains(instance5.LocalFindValue(key), "2") {
		t.Error("Instance 5 didn't store the proper value")
		return
	}
	if !strings.Contains(instance6.LocalFindValue(key), "2") {
		t.Error("Instance 6 didn't store the proper value")
		return
	}
}
func TestDoIterativeFindValue(t *testing.T) {
	//search for nonexistent key
	if !strings.Contains(instance1.DoIterativeFindValue(NewRandomID()), "ERR") {
		t.Error("search for nonexistent key failuee")
		return
	}
	//search for existent key
	key := NewRandomID()
	value := []byte("seven")
	instance1.DoStore(&instance7.SelfContact, key, value)
	s := instance5.DoIterativeFindValue(key)
	if !strings.Contains(s, "found value: seven") {
		t.Error("iterative findvalue failure")
		return
	}
}
