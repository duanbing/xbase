package xbase

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

const (
	dbPath   = "/tmp/xbase"
	tmpPath  = "/tmp/xbase_tmp.txt"
	tmpPath2 = "/tmp/xbase_tmp2.txt"
)

func TestPut(t *testing.T) {
	xb := NewXBase(dbPath, nil)
	defer xb.Close()
	k := []byte("a")
	v := []byte("1")
	if err := xb.Put(k, v); err != nil {
		panic(err)
	}

	//	xb.PrettyPrint(xb.root, 0, 1)

	k = []byte("b")
	v = []byte("1")
	if err := xb.Put(k, v); err != nil {
		panic(err)
	}

	//	xb.PrettyPrint(xb.root, 0, 1)

	k = []byte("ab")
	v = []byte("1")
	if err := xb.Put(k, v); err != nil {
		panic(err)
	}

	//	xb.PrettyPrint(xb.root, 0, 1)
	rootByte := xb.Commit()

	t.Logf("root.seq: %x", rootByte)
	ioutil.WriteFile(tmpPath, []byte(fmt.Sprintf("%x", rootByte)), os.ModePerm)
}

func Test2Put(t *testing.T) {
	//root := "be69daa92c5e771de594f98fb266309be9af203fefa922a6134a21c851dc9aa1"
	root, err := ioutil.ReadFile(tmpPath)
	if err != nil {
		t.Fatal(err)
	}
	xb := NewXBase(dbPath, MustHexDecode([]byte(root)))
	defer xb.Close()
	k := []byte("abc")
	v := []byte("1")
	if err := xb.Put(k, v); err != nil {
		panic(err)
	}
	//	xb.PrettyPrint(xb.root, 0, 1)
	rootByte := xb.Commit()
	t.Logf("root.seq: %x", rootByte)
	ioutil.WriteFile(tmpPath, []byte(fmt.Sprintf("%x", rootByte)), os.ModePerm)
}

func TestGet(t *testing.T) {
	//root := "3b0c14023a5bd3bd9b1c905694904430088b427e3c274ca0a4772d456cc7bda0"
	root, err := ioutil.ReadFile(tmpPath)
	if err != nil {
		t.Fatal(err)
	}
	xb := NewXBase(dbPath, MustHexDecode([]byte(root)))
	defer xb.Close()
	k := []byte("a")
	v := []byte("1")
	if vInDb, err := xb.Get(k); err != nil {
		panic(err)
	} else if !bytes.Equal(v, vInDb) {
		t.Fatalf("unequal : get %s", vInDb)
	}

	k = []byte("b")

	if vInDb, err := xb.Get(k); err != nil {
		panic(err)
	} else if !bytes.Equal(v, vInDb) {
		t.Fatalf("unequal : get %s", vInDb)
	}

	k = []byte("ab")
	if vInDb, err := xb.Get(k); err != nil {
		panic(err)
	} else if !bytes.Equal(v, vInDb) {
		t.Fatalf("unequal : get %s", vInDb)
	}

	k = []byte("abc")
	if vInDb, err := xb.Get(k); err != nil {
		panic(err)
	} else if !bytes.Equal(v, vInDb) {
		t.Fatalf("unequal : get %s", vInDb)
	}

	rootByte := xb.Commit()
	t.Logf("root.seq: %x", rootByte)
	ioutil.WriteFile(tmpPath, []byte(fmt.Sprintf("%x", rootByte)), os.ModePerm)
}

// 生成32位MD5
func MD5(text string) string {
	ctx := md5.New()
	ctx.Write([]byte(text))
	return hex.EncodeToString(ctx.Sum(nil))
}

func BenchmarkPutPerf(t *testing.B) {
	//root := "3b0c14023a5bd3bd9b1c905694904430088b427e3c274ca0a4772d456cc7bda0"
	root, err := ioutil.ReadFile(tmpPath)
	if err != nil {
		t.Fatal(err)
	}
	xb := NewXBase(dbPath, MustHexDecode([]byte(root)))
	defer xb.Close()

	rounds := 100000
	var lastKey []byte
	for i := 0; i < rounds; i++ {
		k := []byte(MD5(strings.Repeat(fmt.Sprintf("%d", i), 10)))
		lastKey = k
		v := []byte("1")

		if err := xb.Put(k, v); err != nil {
			t.Fatalf("k=%s,v=%s,error=%s", k, v, err.Error())
		}

		if i%3 == 1 {
			if vInDb, err := xb.Get(lastKey); err != nil {
				panic(err)
			} else if !bytes.Equal(v, vInDb) {
				t.Fatalf("unequal: get %s", vInDb)
			}
		}
	}
	rootByte := xb.Commit()
	t.Logf("root.seq: %x", rootByte)
	ioutil.WriteFile(tmpPath, []byte(fmt.Sprintf("%x", rootByte)), os.ModePerm)
}

func TestDel(t *testing.T) {
	root, err := ioutil.ReadFile(tmpPath)
	if err != nil {
		t.Fatal(err)
	}
	xb := NewXBase(dbPath, MustHexDecode([]byte(root)))
	defer xb.Close()

	k := []byte("a")
	if err := xb.Delete(k); err != nil {
		panic(err)
	}

	if v, err := xb.Get(k); err == nil {
		panic(fmt.Errorf("should be not found, %s", v))
	}
	//	xb.PrettyPrint(xb.root, 0, 1)
	rootByte := xb.Commit()
	t.Logf("root.seq: %x", rootByte)
	ioutil.WriteFile(tmpPath, []byte(fmt.Sprintf("%x", rootByte)), os.ModePerm)
	ioutil.WriteFile(tmpPath2, []byte(fmt.Sprintf("%x", rootByte)), os.ModePerm)
}

func Test2Del(t *testing.T) {
	root, err := ioutil.ReadFile(tmpPath)
	if err != nil {
		t.Fatal(err)
	}
	xb := NewXBase(dbPath, MustHexDecode([]byte(root)))
	defer xb.Close()

	k := []byte("a")
	if v, err := xb.Get(k); err == nil {
		panic(fmt.Errorf("should be not found, %s", v))
	}

	v := []byte("1")
	if err := xb.Put(k, v); err != nil {
		t.Fatal(err)
	}
	rootByte := xb.Commit()
	t.Logf("root.seq: %x", rootByte)
	ioutil.WriteFile(tmpPath, []byte(fmt.Sprintf("%x", rootByte)), os.ModePerm)
}

func Test3DelFromOldBranch(t *testing.T) {
	root, err := ioutil.ReadFile(tmpPath2)
	if err != nil {
		t.Fatal(err)
	}
	xb := NewXBase(dbPath, MustHexDecode([]byte(root)))
	defer xb.Close()

	k := []byte("a")
	if v, err := xb.Get(k); err == nil {
		panic(fmt.Errorf("should be not found, %s", v))
	}

	v := []byte("1")
	if err := xb.Put(k, v); err != nil {
		t.Fatal(err)
	}
	rootByte := xb.Commit()
	t.Logf("root.seq: %x", rootByte)
}
