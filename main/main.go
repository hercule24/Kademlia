// ltw398 yco664 bwl614
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
)

import (
	"kademlia"
)

//for locking vdo_map
//var vdo_mutex = &sync.Mutex{}

//for locking kademlia
//var k_mutex = &sync.Mutex{}

func main() {
	// By default, Go seeds its RNG with 1. This would cause every program to
	// generate the same sequence of IDs. Use the current nano time to
	// random numbers
	rand.Seed(time.Now().UnixNano())

	// Get the bind and connect connection strings from command-line arguments.
	flag.Parse()
	args := flag.Args()
	if len(args) != 2 {
		log.Fatal("Must be invoked with exactly two arguments!\n")
	}
	listenStr := args[0]
	firstPeerStr := args[1]

	// Create the Kademlia instance
	fmt.Printf("kademlia starting up!\n")
	kadem := kademlia.NewKademlia(listenStr)

	// Confirm our server is up with a PING request and then exit.
	// Your code should loop forever, reading instructions from stdin and
	// printing their results to stdout. See README.txt for more details.

	//fmt.Println("before the dial")

	_, port, _ := net.SplitHostPort(firstPeerStr)
	client, err := rpc.DialHTTPPath("tcp", firstPeerStr, rpc.DefaultRPCPath+port)
	//client, err := rpc.DialHTTP("tcp", firstPeerStr)
	if err != nil {
		log.Fatal("DialHTTP: ", err)
	}

	
	ping := new(kademlia.PingMessage)

	
	ping.Sender = kadem.SelfContact

	ping.MsgID = kademlia.NewRandomID()
	var pong kademlia.PongMessage

	//fmt.Println("before the call")

	err = client.Call("KademliaCore.Ping", ping, &pong)

	// added by me
	kadem.Update(&pong.Sender)

	if err != nil {
		log.Fatal("Call: ", err)
	}

	log.Printf("ping msgID: %s\n", ping.MsgID.AsString())
	log.Printf("pong msgID: %s\n", pong.MsgID.AsString())

	in := bufio.NewReader(os.Stdin)
	quit := false
	for !quit {
		line, err := in.ReadString('\n')
		if err != nil {
			log.Fatal(err)
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		resp := executeLine(kadem, line)
		if resp == "quit" {
			quit = true
		} else if resp != "" {
			fmt.Printf("%v\n", resp)
		}
	}
}

func executeLine(k *kademlia.Kademlia, line string) (response string) {
	toks := strings.Fields(line)
	switch {
	case toks[0] == "quit":
		response = "quit"
	case toks[0] == "whoami":
		if len(toks) > 1 {
			response = "usage: whoami"
			return
		}
		response = k.NodeID.AsString()

	case toks[0] == "print_contact":
		if len(toks) < 2 || len(toks) > 2 {
			response = "usage: print_contact [nodeID]"
			return
		}
		id, err := kademlia.IDFromString(toks[1])
		if err != nil {
			response = "ERR: Not a valid node ID (" + toks[1] + ")"
			return
		}
		c, err := k.FindContact(id)
		if err != nil {
			response = "ERR: Unknown contact node ID"
			return
		}
		response = "OK: NodeID=" + toks[1] + "\n"
		response += "      Host=" + c.Host.String() + "\n"
		response += "      Port=" + strconv.Itoa(int(c.Port))
	case toks[0] == "ping":
		// Do a ping
		//
		// Check if toks[1] is a valid NodeID, if not, try pinging host:port
		// print an error if neither is valid
		//
		// Following lines need to be expanded
		//fmt.Println("inside ping")

		if len(toks) < 2 || len(toks) > 2 {
			response = "usage: ping [nodeID | host:port]"
			return
		}
		id, err := kademlia.IDFromString(toks[1])
		if err != nil {
			hostname, portstr, err := net.SplitHostPort(toks[1])
			if err != nil {
				response = "ERR: Not a valid Node ID or host:port address"
				return
			}
			port, err := strconv.Atoi(portstr)
			if err != nil {
				response = "ERR: Not a valid Node ID or host:port address"
				return
			}
			ipAddrStrings, err := net.LookupHost(hostname)
			if err != nil {
				response = "ERR: Could not find the provided hostname"
				return
			}
			var host net.IP
			for i := 0; i < len(ipAddrStrings); i++ {
				host = net.ParseIP(ipAddrStrings[i])
				if host.To4() != nil {
					break
				}
			}
			response = k.DoPing(host, uint16(port))
			return
		}
		c, err := k.FindContact(id)
		if err != nil {
			fmt.Println("find contact return non nil")
			response = "ERR: Not a valid Node ID or host:port address"
			return
		}
		response = k.DoPing(c.Host, c.Port)

	case toks[0] == "local_find_value":
		// print a local variable
		if len(toks) < 2 || len(toks) > 2 {
			response = "usage: local_find_value [key]"
			return
		}
		key, err := kademlia.IDFromString(toks[1])
		if err != nil {
			response = "ERR: Provided an invalid key (" + toks[1] + ")"
			return
		}
		response = k.LocalFindValue(key)

	case toks[0] == "store":
		// Store key, value pair at NodeID
		if len(toks) < 4 || len(toks) > 4 {
			response = "usage: store [nodeID] [key] [value]"
			return
		}
		nodeId, err := kademlia.IDFromString(toks[1])
		if err != nil {
			response = "ERR: Provided an invalid node ID (" + toks[1] + ")"
			return
		}
		contact, err := k.FindContact(nodeId)
		if err != nil {
			response = "ERR: Unable to find contact with node ID (" + toks[1] + ")"
			return
		}
		key, err := kademlia.IDFromString(toks[2])
		if err != nil {
			response = "ERR: Provided an invalid key (" + toks[2] + ")"
			return
		}
		value := []byte(toks[3])

		response = k.DoStore(contact, key, value)

	case toks[0] == "find_node":
		// perform a find_node RPC
		if len(toks) < 3 || len(toks) > 3 {
			response = "usage: find_node [nodeID] [key]"
			return
		}

		nodeId, err := kademlia.IDFromString(toks[1])
		if err != nil {
			response = "ERR: Provided an invalid node ID (" + toks[1] + ")"
			return
		}
		contact, err := k.FindContact(nodeId)
		if err != nil {
			response = "ERR: Unable to find contact with node ID (" + toks[1] + ")"
			return
		}
		key, err := kademlia.IDFromString(toks[2])
		if err != nil {
			response = "ERR: Provided an invalid key (" + toks[2] + ")"
			return
		}
		response = k.DoFindNode(contact, key)

	case toks[0] == "find_value":
		// perform a find_value RPC
		if len(toks) < 3 || len(toks) > 3 {
			response = "usage: find_value [nodeID] [key]"
			return
		}

		nodeId, err := kademlia.IDFromString(toks[1])
		if err != nil {
			response = "ERR: Provided an invalid node ID (" + toks[1] + ")"
			return
		}
		contact, err := k.FindContact(nodeId)
		if err != nil {
			response = "ERR: Unable to find contact with node ID (" + toks[1] + ")"
			return
		}
		key, err := kademlia.IDFromString(toks[2])
		if err != nil {
			response = "ERR: Provided an invalid key (" + toks[2] + ")"
			return
		}
		response = k.DoFindValue(contact, key)

	case toks[0] == "iterativeFindNode":
		//fmt.Println("inside iterativeFindNode")
		// perform an iterative find node
		//if len(toks) < 2 || len(toks) > 2 {
		if len(toks) != 2 {
			response = "usage: iterativeFindNode [nodeID]"
			return
		}
		//id, err := kademlia.IDFromString(toks[2])
		id, err := kademlia.IDFromString(toks[1])
		if err != nil {
			response = "ERR: Provided an invalid node ID(" + toks[1] + ")"
			return
		}
		for _, v := range k.DoIterativeFindNode(id) {
			response += v.NodeID.AsString() + "\n"

		}
		//fmt.Println("our response")
		//fmt.Println(response)

	case toks[0] == "iterativeStore":
		// perform an iterative store
		//log.Printf("inside DoIterativeStore")

		//if len(toks) < 2 || len(toks) > 2 {
		if len(toks) != 3 {
			response = "usage: iterativeStore [key] [value]"
			return
		}
		key, err := kademlia.IDFromString(toks[1])
		if err != nil {
			//response = "ERR: Provided an invalid key (" + toks[2] + ")"
			response = "ERR: Provided an invalid key (" + toks[1] + ")"
			return
		}
		response = k.DoIterativeStore(key, []byte(toks[2]))

	case toks[0] == "iterativeFindValue":
		//fmt.Println("inside iterativeFindValue")
		// performa an iterative find value
		//if len(toks) < 2 || len(toks) > 2 {
		if len(toks) != 2 {
			response = "usage: iterativeFindValue [key]"
			return
		}
		//key, err := kademlia.IDFromString(toks[2])
		key, err := kademlia.IDFromString(toks[1])
		if err != nil {
			response = "ERR: Provided an invalid key (" + toks[1] + ")"
			return
		}
		response = k.DoIterativeFindValue(key)

	case toks[0] == "vanish":
		if len(toks) != 5 {
			response = "usage: vanish [VDO ID] [data] [numberKeys] [threshold]"
			return
		}
		VdoID, err := kademlia.IDFromString(toks[1])
		if err != nil {
			response = "ERR: Provided an invalid VdoID (" + toks[1] + ")"
			return
		}
		N, err := strconv.Atoi(toks[3])
		if err != nil {
			response = "ERR: Provided an invalid N (" + toks[3] + ")"
			return
		}
		T, err := strconv.Atoi(toks[4])
		if err != nil {
			response = "ERR: Provided an invalid T (" + toks[4] + ")"
			return
		}
		vdo := kademlia.VanishData(*k, []byte(toks[2]), byte(N), byte(T))

		//store VDO locally
		response = k.StoreVDO(VdoID, vdo)
		

	case toks[0] == "unvanish":
		if len(toks) != 3 {
			response = "usage: unvanish [Node ID] [VDO ID]"
			return
		}
		NodeID, err := kademlia.IDFromString(toks[1])
		if err != nil {
			response = "ERR: Provided an invalid Node ID (" + toks[1] + ")"
			return
		}
		VdoID, err := kademlia.IDFromString(toks[2])
		if err != nil {
			response = "ERR: Provided an invalid VDO ID (" + toks[2] + ")"
			return
		}
		response = k.DoGETVDO(NodeID, VdoID)
		

	default:
		response = "ERR: Unknown command"
	}
	return
}
