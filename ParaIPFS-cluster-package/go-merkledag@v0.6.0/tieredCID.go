// Package merkledag implements the IPFS Merkle DAG data structures.
package merkledag

import (
	"fmt"
	"os"
	"sync"

	cid "github.com/ipfs/go-cid"
)

var PinBuffer map[cid.Cid]*TierCid
var PinBufferMutex *sync.Mutex
var UnPinBuffer map[cid.Cid]*TierCid
var UnPinBufferMutex *sync.Mutex

var NumThread = 64 * 8


func init() {
	PinBuffer = make(map[cid.Cid]*TierCid)
	PinBufferMutex = &sync.Mutex{}
}
// var IPFS_Path = "/home/mssong/.ipfs"
// var IPFS_Path = "/mnt/nvme0n1/.ipfs"

// var IPFS_Path = "/root/.ipfs"
// var IPFS_DownloadPath = "/root/ipfs_client"

var IPFS_Path = "/root/ipfs_server/.ipfs"
var IPFS_DownloadPath = "/root/ipfs_server" //md0

type TierCid struct {
	NonLeaf []cid.Cid
	Leaf    []cid.Cid
}

func (tc *TierCid) Print() {
	for i := 0; i < len(tc.NonLeaf); i++ {

	}
	for i := 0; i < len(tc.Leaf); i++ {

	}
}

func NewTierCid() *TierCid {
	tc := new(TierCid)
	tc.NonLeaf = make([]cid.Cid, 0)
	tc.Leaf = make([]cid.Cid, 0)
	return tc
}

func PrintPinBuffer(cid cid.Cid) {
	tc := PinBuffer[cid]
	fmt.Println("----------Print PinBuffer(CID: %s)\n----------", cid.String())
	fmt.Printf("CID:%s\n", cid.String())
	fmt.Printf("NonLeaf:\n")
	for i := 0; i < len(tc.NonLeaf); i++ {
		fmt.Println(tc.NonLeaf[i])
	}
	fmt.Printf("Leaf:\n")
	for i := 0; i < len(tc.Leaf); i++ {
		fmt.Println(tc.Leaf[i])
	}
	fmt.Println("------------------------------")
}

func PrintAllPinBuffer() {
	f, err := os.Create("/home/mssong/PrintAllPinBuffer")
	if err != nil {
		panic(err)
	}
	fmt.Fprintf(f, "----------Print PinBuffer----------\n")

	for cid, tc := range PinBuffer {
		fmt.Fprintf(f, "==CID:%s, ==len(NonLeaf):%d, ==len(Leaf):%d\n", cid.String(), len(tc.NonLeaf), len(tc.Leaf))
		fmt.Fprintf(f, "\tNonLeaf:\n")
		for i := 0; i < len(tc.NonLeaf); i++ {
			fmt.Fprintf(f, "\t%s\n", tc.NonLeaf[i].String())
		}
		fmt.Fprintf(f, "\tLeaf:\n")
		for i := 0; i < len(tc.Leaf); i++ {
			fmt.Fprintf(f, "\t%s\n", tc.Leaf[i].String())
		}
	}
	fmt.Fprintf(f, "------------------------------")

}

func PrintUnPinBuffer(cid cid.Cid) {
	tc := UnPinBuffer[cid]
	fmt.Println("----------Print UnPinBuffer(CID: %s)\n----------", cid.String())
	fmt.Printf("NonLeaf:\n")
	for i := 0; i < len(tc.NonLeaf); i++ {
		fmt.Println(tc.NonLeaf[i])
	}
	fmt.Printf("Leaf:\n")
	for i := 0; i < len(tc.Leaf); i++ {
		fmt.Println(tc.Leaf[i])
	}
	fmt.Println("------------------------------")
}

func UnPrintAllPinBuffer() {
	f, err := os.Create("/home/mssong/UnPrintAllPinBuffer")
	if err != nil {
		panic(err)
	}
	fmt.Fprintf(f, "----------Print UnPinBuffer----------\n")

	for cid, tc := range UnPinBuffer {
		fmt.Fprintf(f, "==CID:%s, ==len(NonLeaf):%d, ==len(Leaf):%d\n", cid.String(), len(tc.NonLeaf), len(tc.Leaf))
		fmt.Fprintf(f, "\tNonLeaf:\n")
		for i := 0; i < len(tc.NonLeaf); i++ {
			fmt.Fprintf(f, "\t%s\n", tc.NonLeaf[i].String())
		}
		fmt.Fprintf(f, "\tLeaf:\n")
		for i := 0; i < len(tc.Leaf); i++ {
			fmt.Fprintf(f, "\t%s\n", tc.Leaf[i].String())
		}
	}
	fmt.Fprintf(f, "------------------------------")

}
