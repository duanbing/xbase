package xbase

import (
	"encoding/hex"
	"log"
	"os"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
)

func MustHexEncode(src []byte) []byte {
	sz := hex.EncodedLen(len(src))
	dst := make([]byte, sz)
	hex.Encode(dst, src)
	return dst
}

func MustHexEncodeWithEOF(src []byte) []byte {
	dst := MustHexEncode(src)
	return append(dst, indicesEOF)
}

func MustHexDecode(src []byte) []byte {
	dst := make([]byte, hex.DecodedLen(len(src)))
	n, err := hex.Decode(dst, src)
	if err != nil {
		panic(err)
	}
	return dst[0:n]
}

var (
	indicesMap map[byte]int = map[byte]int{
		'0': 0,
		'1': 1,
		'2': 2,
		'3': 3,
		'4': 4,
		'5': 5,
		'6': 6,
		'7': 7,
		'8': 8,
		'9': 9,
		'a': 10,
		'b': 11,
		'c': 12,
		'd': 13,
		'e': 14,
		'f': 15,
		'g': 16,
	}

	indicesEOF byte = 'g'
)

type XBase struct {
	root *Node
	db   *leveldb.DB
	log  *log.Logger
	lock sync.RWMutex
}

func NewXBase(path string, root []byte) *XBase {
	// set root as a NOD, root is always a NOD
	n := newNode(nil)
	xbase := &XBase{}
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		panic(err)
	}
	xbase.db = db
	xbase.log = log.New(os.Stderr, "[XBase]", 0)

	if root != nil {
		rootBytes, err := db.Get(root, nil)
		if err != nil {
			panic(err)
		}
		xbase.log.Printf("get root: %x\n", rootBytes)
		//get root from
		if err := n.FromBytes(rootBytes); err != nil {
			panic(err)
		}
	}
	xbase.root = n
	return xbase
}

func (xbase *XBase) Put(key, value []byte) error {
	xbase.lock.Lock()
	defer xbase.lock.Unlock()
	xbase.ins(MustHexEncodeWithEOF(key), value)
	return nil
}

func (xbase *XBase) Get(key []byte) ([]byte, error) {
	xbase.lock.RLock()
	defer xbase.lock.RUnlock()
	return xbase.get(MustHexEncodeWithEOF(key))
}

func (xbase *XBase) Delete(key []byte) error {
	xbase.lock.Lock()
	defer xbase.lock.Unlock()
	return xbase.del(MustHexEncodeWithEOF(key))
}

func (xbase *XBase) Commit() []byte {
	xbase.lock.RLock()
	defer xbase.lock.RUnlock()
	batchWrite := new(leveldb.Batch)
	var isdirty bool
	hash := xbase.commit(xbase.root, batchWrite, &isdirty)
	if isdirty {
		xbase.log.Printf("dirty root, begin to write batch")
		if err := xbase.db.Write(batchWrite, nil); err != nil {
			panic(err)
		}
	}
	return hash
}

func (xbase *XBase) Close() {
	if xbase.db != nil {
		xbase.db.Close()
	}
}
