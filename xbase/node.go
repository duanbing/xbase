package xbase

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	"github.com/syndtr/goleveldb/leveldb"
)

const (
	NODECOUNT = 17
)

type NodeType uint8

const (
	NIL  NodeType = iota // NIL NODE: initial status of a node
	LEAF                 // leaf node: there are no children nodes.
	REF                  // reference node:  this kinda node is loaded in memory, but only seq is defined. the other fields got loaded  or committed only if this node become dirty
	NOD                  // branch node, there are some children nodes.
)

var (
	SHA256_NIL [sha256.Size]byte = sha256.Sum256([]byte(""))

	ErrNotFound         = errors.New("XBase Not Found")
	ErrInvalidBytesRead = errors.New("Invalid bytes read")
	ErrUnreachable      = errors.New("can't reach here")
)

// Optimize: only NOD need this structure.  NIL , LEAF can be []byte,but implementing the Bytes and FromBytes
type Node struct {
	nodes      [NODECOUNT]*Node
	typ        NodeType
	key, value []byte            // key is the prefix of a node
	seq        [sha256.Size]byte // the unique ID of a node, if node is LEAF, seq = sha256(key), or seq = sha256(concat[children seq])
	dirty      bool
}

func (n *Node) Bytes() []byte {
	var b bytes.Buffer
	// size, [children idx,...], [children seq,...]
	var nodeIdx []int32
	var nodeBuf [][]byte
	cnt := 0
	for i := 0; i < len(n.nodes); i++ {
		if n.nodes[i] != nil && n.nodes[i].typ != NIL {
			nodeIdx = append(nodeIdx, int32(i))
			nodeBuf = append(nodeBuf, n.nodes[i].seq[:])
			cnt += 1
		}
	}
	binary.Write(&b, binary.BigEndian, int32(len(nodeIdx)))
	for i := 0; i < cnt; i++ {
		binary.Write(&b, binary.BigEndian, int32(nodeIdx[i]))
	}
	for i := 0; i < cnt; i++ {
		b.Write(nodeBuf[i])
	}

	// node_type
	binary.Write(&b, binary.BigEndian, int32(n.typ))

	// size_key, key, size_value, value
	binary.Write(&b, binary.BigEndian, int32(len(n.key)))
	b.Write(n.key)
	binary.Write(&b, binary.BigEndian, int32(len(n.value)))
	b.Write(n.value)

	return b.Bytes()
}

func BytesToInt(b []byte) int {
	bytesBuffer := bytes.NewBuffer(b)
	var x int32
	binary.Read(bytesBuffer, binary.BigEndian, &x)
	return int(x)
}

func (node *Node) FromBytes(bt []byte) error {
	b := bytes.NewReader(bt)
	lenBuf := make([]byte, 4)
	nodeBuf := make([]byte, 32)

	if n, err := b.Read(lenBuf); err != nil {
		return err
	} else if n != 4 {
		return ErrInvalidBytesRead
	} else {
		childrenNum := BytesToInt(lenBuf)
		//		fmt.Println("children: ", childrenNum)
		var idxBuf []int
		for i := 0; i < childrenNum; i++ {
			if n, err := b.Read(lenBuf); err != nil {
				return err
			} else if n != 4 {
				return ErrInvalidBytesRead
			} else {
				idxBuf = append(idxBuf, BytesToInt(lenBuf))
				//				fmt.Printf("idx %#v\n", idxBuf)
			}
		}

		for i := 0; i < childrenNum; i++ {
			if n, err := b.Read(nodeBuf); err != nil {
				return err
			} else if n != 32 {
				return ErrInvalidBytesRead
			} else {
				node.nodes[idxBuf[i]] = &Node{
					typ: REF, // for unloaded node, set REF
				}
				copy(node.nodes[idxBuf[i]].seq[:], nodeBuf)
				//				fmt.Printf("nodes %d : %x\n", idxBuf[i], node.nodes[idxBuf[i]].seq)
			}
		}
		// refill the nil children
		j := 0
		//		fmt.Printf("refill: ")
		for i := 0; i < NODECOUNT; i++ {
			if j < len(idxBuf) && i == idxBuf[j] {
				j++
			} else {
				//				fmt.Printf("%d,", i)
				node.nodes[i] = newNode(nil)
			}
		}
		//		fmt.Println("")
		if j != len(idxBuf) {
			panic(ErrUnreachable)
		}
	}
	if n, err := b.Read(lenBuf); err != nil {
		return err
	} else if n != 4 {
		return ErrInvalidBytesRead
	} else {
		node.typ = NodeType(BytesToInt(lenBuf))
		//fmt.Printf("type : %d\n", node.typ)
	}

	if n, err := b.Read(lenBuf); err != nil {
		return err
	} else if n != 4 {
		return ErrInvalidBytesRead
	} else {
		keyLen := BytesToInt(lenBuf)
		node.key = make([]byte, keyLen)
		if keyLen > 0 {
			node.key = make([]byte, keyLen)
			if n, err := b.Read(node.key); err != nil {
				return err
			} else if n != keyLen {
				return ErrInvalidBytesRead
			}
		}
	}

	if n, err := b.Read(lenBuf); err != nil {
		return err
	} else if n != 4 {
		return ErrInvalidBytesRead
	} else {
		keyLen := BytesToInt(lenBuf)
		//		fmt.Println("value length", keyLen)
		node.value = make([]byte, keyLen)
		if keyLen > 0 {
			node.value = make([]byte, keyLen)
			if n, err := b.Read(node.value); err != nil {
				return err
			} else if n != keyLen {
				return ErrInvalidBytesRead
			}
		}
	}
	//	fmt.Printf("FromBytes key=%s,seq=%x\n", node.key, node.value)
	return nil
}

func newNode(ck []byte) *Node {
	buf := make([]byte, len(ck))
	copy(buf, ck)
	n := &Node{
		key: buf,
		typ: NIL,
	}
	for i := 0; i < NODECOUNT; i++ {
		n.nodes[i] = &Node{
			typ: NIL,
		}
	}
	return n
}

func openNode(n *Node, db *leveldb.DB, key []byte) {
	nodesStr, err := db.Get(key, nil)
	if err != nil {
		panic(err)
	}
	n.FromBytes(nodesStr)
	//	fmt.Printf("open Node, key=%s, value=%s\n", n.key, n.value)
}

// prefixLen returns the maximum length of the common prefix of a and b.
func prefixLen(a, b []byte) int {
	var i, length = 0, len(a)
	if len(b) < length {
		length = len(b)
	}
	for ; i < length; i++ {
		if a[i] != b[i] {
			break
		}
	}
	return i
}

func (xb *XBase) PrettyPrint(curNode *Node, kid int, dep int) {
	/*
		// open the comment if want to print the whole tree
			if curNode.typ == REF {
					curNode = openNode(xb.db, curNode.seq[:])
			}
	*/
	fmt.Printf("kid:%02d,%s,key=%s[type=%d, isdirty=%t]\n", kid, strings.Repeat(".", dep), curNode.key, curNode.typ, curNode.dirty)
	for i := 0; i < NODECOUNT; i++ {
		if curNode.nodes[i] != nil && curNode.nodes[i].typ != NIL {
			xb.PrettyPrint(curNode.nodes[i], i, dep+1)
		}
	}
}

// key and value should be in hex format
func (xb *XBase) ins(key []byte, value []byte) {
	//xb.log.Printf("put string: [%s]", key)
	curNode := xb.root
	for i := 0; i < len(key); {
		ik := indicesMap[key[i]]
		//		xb.log.Printf("now i=%d,ik=%d, key=%s,typ=%d", i, ik, curNode.key, curNode.typ)
		//		xb.PrettyPrint(xb.root, 0, 1)
		kidNode := curNode.nodes[ik]
		if kidNode.typ == REF {
			openNode(kidNode, xb.db, kidNode.seq[:])
			continue
		}
		prevIdx := ik
		pl := prefixLen(key[i:], kidNode.key)
		//		xb.log.Printf("kid index=%d,prefixLen(%s,%s)=%d, i=%d", ik, key[i:], kidNode.key, pl, i)
		if pl == 0 {
			curNode.nodes[ik] = newNode(key[i:])
			curNode.typ = NOD // root node assigned to NOD specially
			curNode = curNode.nodes[ik]
			//			xb.log.Printf("over, i=%d,key=%s", i, key[i:])
			i = len(key)
			continue
		} else if pl >= len(kidNode.key) {
			curNode = kidNode
			i = i + pl
			//			xb.log.Printf("key includes curNode.key, i=%d,ik=%d,key=%s", i, i+pl, key[i:])
			continue
		} else {
			//xb.log.Printf("kidNode.key=%s,i=%d,pl=%d", kidNode.key, i, pl)
			kidNode.key = kidNode.key[pl:]
			//xb.log.Printf("kidNode.key=%s", kidNode.key)
			nNode := newNode(key[i : i+pl])
			//xb.log.Printf("nNode.key=%s", nNode.key)
			nNode.typ = NOD
			nNode.dirty = true

			ik = indicesMap[kidNode.key[0]]
			nNode.nodes[ik] = kidNode

			curNode.nodes[prevIdx] = nNode
			//xb.log.Printf("prevIdx=%d", prevIdx)

			curNode = nNode
			i = i + pl
			//xb.log.Printf("key partly includes curNode.key, i=%d,key=%s", i, key[i:])
			continue
		}
	}
	curNode.value = make([]byte, len(value))
	copy(curNode.value, value)
	curNode.typ = LEAF
	curNode.dirty = true
	//	fmt.Printf("ins leaf %s=%s\n", curNode.key, curNode.value)
}

func (xb *XBase) get(key []byte) ([]byte, error) {
	curNode := xb.root
	//	xb.log.Printf("real get %s\n", key)
	var pl int
	for i := 0; i < len(key); {
		ik := indicesMap[key[i]]
		//		xb.log.Printf("get : i=%d, ik=%d, type=%d, key=%s", i, ik, curNode.typ, curNode.key)
		curNode = curNode.nodes[ik]
		if curNode == nil {
			return nil, ErrNotFound
		}
		for curNode.typ == REF {
			//			xb.log.Printf("REF %x", curNode.seq)
			openNode(curNode, xb.db, curNode.seq[:])
		}
		//			xb.log.Printf("TYPE %d [%s] [%s]\n", curNode.typ, curNode.key, curNode.value)
		pl = prefixLen(curNode.key, key[i:])
		i += pl
		//		xb.log.Printf("curNode.key=%s, i = %d, pl=%d", curNode.key, i, pl)
	}
	//	xb.log.Printf("end, i=%d, len(key)=%d,,typ=%d %s=%s", i, len(key), curNode.typ, curNode.key, curNode.value)

	for {
		if curNode.typ == LEAF {
			return curNode.value, nil
		} else if curNode.typ == REF {
			//			xb.log.Printf("get REF,typ=%d %s=%s seq=%x", curNode.typ, curNode.key, curNode.value, curNode.seq)
			openNode(curNode, xb.db, curNode.seq[:])
		} else {
			//xb.log.Printf("get finally,typ=%d %s=%s", curNode.typ, curNode.key, curNode.value)
			break
		}
	}
	return nil, ErrNotFound
}

func (xb *XBase) del(key []byte) error {
	curNode := xb.root
	var pl int
	for i := 0; i < len(key); {
		ik := indicesMap[key[i]]
		curNode = curNode.nodes[ik]
		for curNode.typ == REF {
			openNode(curNode, xb.db, curNode.seq[:])
		}
		pl = prefixLen(curNode.key, key[i:])
		i += pl
	}
	for {
		if curNode.typ == LEAF {
			//Deletion condition:
			//xb.log.Printf("deleted")
			curNode.typ = NIL
			curNode.dirty = true
			return nil
		} else if curNode.typ == REF {
			openNode(curNode, xb.db, curNode.seq[:])
		} else {
			xb.log.Printf("unexisted not")
			break
		}
	}
	xb.log.Printf("unexisted end")
	return nil
}

//commit return seqID of the tree
func (xb *XBase) commit(curNode *Node, batch *leveldb.Batch, isdirty *bool) []byte {
	var data []byte
	//bData := false
	realDirty := false
	if curNode.typ == NIL {
		if curNode.dirty {
			*isdirty = true
		}
		goto NIL_RET
	}
	if curNode.typ == LEAF {
		data = append(data, curNode.key...)
		//	bData = true
		goto DO_RET
	}

	if curNode.typ == REF {
		*isdirty = false
		return curNode.seq[:]
	}
	for i := 0; i < NODECOUNT; i++ {
		nNode := curNode.nodes[i]
		if nNode != nil {
			if nNode.typ != NIL {
				data = append(data, xb.commit(nNode, batch, &realDirty)...)
			}
			if nNode.dirty || realDirty {
				*isdirty = true
			}
		}
	}

DO_RET:
	curNode.seq = sha256.Sum256(data)
	if curNode.dirty || *isdirty {
		//		xb.log.Printf("batch add: %x(%s=%s, isData=%t, typ=%d): %x", curNode.seq[:], curNode.key, curNode.value, bData, curNode.typ, curNode.Bytes())
		batch.Put(curNode.seq[:], curNode.Bytes())
		*isdirty = true
	}
	return curNode.seq[:]
NIL_RET:
	panic(ErrUnreachable)
	return nil
}
