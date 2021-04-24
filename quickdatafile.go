// quickdatafile project quickdatafile.go
package quickdatafile

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math/rand"
	"orderfilev4sim"
	"os"
	"strconv"
	"sync"
	"time"
)

type QuickDataFile struct {
	dbmem    []byte
	path     string
	pathfile *os.File
	mu       sync.RWMutex
	filemu   sync.RWMutex
	dbclosed bool
}

//tool slow for >100000. because rewrite index tool big.
func NewQuickDataFile(path string) *QuickDataFile {
	db := &QuickDataFile{path: path}
	var err error
	db.dbmem, err = ioutil.ReadFile(path)
	if err != nil {
		ff, ffe := os.Create(path)
		if ffe == nil {
			ff.Close()
		}
	}
	db.pathfile, err = os.OpenFile(db.path+".data", os.O_CREATE|os.O_RDWR, 0666)
	rand.Seed(time.Now().UnixNano())
	if err == nil {
		go dbfilesync(db)
		return db
	} else {
		return nil
	}
}

func dbfilesync(db *QuickDataFile) {
	for true {
		time.Sleep(300 * time.Second)
		if db.dbclosed {
			break
		}
		db.mu.Lock()
		db.pathfile.Sync()
		db.mu.Unlock()
	}
}

func (db *QuickDataFile) Put(key uint64, val []byte) bool {
	keybt := make([]byte, 13)
	db.mu.Lock()
	if len(val) > 0 {
		curpos, err := db.pathfile.Seek(0, os.SEEK_END)
		if err != nil {
			db.mu.Unlock()
			return false
		}
		binary.BigEndian.PutUint32(keybt[:4], uint32(len(val)))
		db.pathfile.Write(keybt[:4])
		db.pathfile.Write(val)
		binary.BigEndian.PutUint64(keybt[len(keybt)-8:], uint64(curpos))
	} else {
		binary.BigEndian.PutUint64(keybt[len(keybt)-8:], ^uint64(0))
	}
	binary.BigEndian.PutUint64(keybt[:8], key)
	if len(db.dbmem) == 0 {
		db.dbmem = append(db.dbmem, keybt...)
	}
	start := 0
	end := len(db.dbmem) / 13
	cur := (start + end) / 2
	for true {
		if key < binary.BigEndian.Uint64(db.dbmem[cur*13:cur*13+8]) {
			end = cur
			cur2 := (start + end) / 2
			if cur2 == cur {
				db.dbmem = append(db.dbmem, keybt...)
				copy(db.dbmem[cur*13+13:], db.dbmem[cur*13:])
				copy(db.dbmem[cur*13:cur*13+13], keybt)
				db.mu.Unlock()
				return true
			} else {
				cur = cur2
			}
		} else if key > binary.BigEndian.Uint64(db.dbmem[cur*13:cur*13+8]) {
			start = cur
			cur2 := (start + end) / 2
			if cur2 == cur {
				db.dbmem = append(db.dbmem, keybt...)
				copy(db.dbmem[cur*13+13+13:], db.dbmem[cur*13+13:])
				copy(db.dbmem[cur*13+13:cur*13+13+13], keybt)
				db.mu.Unlock()
				return true
			} else {
				cur = cur2
			}
		} else {
			copy(db.dbmem[cur*13:cur*13+13], keybt)
			db.mu.Unlock()
			return true
		}
	}
	db.mu.Unlock()
	return false
}

func (db *QuickDataFile) Exists(key uint64) bool {
	db.mu.RLock()
	start := 0
	end := len(db.dbmem) / 13
	if end == 0 {
		db.mu.RUnlock()
		return false
	}
	cur := (start + end) / 2
	for true {
		if key < binary.BigEndian.Uint64(db.dbmem[cur*13:cur*13+8]) {
			end = cur
			cur2 := (start + end) / 2
			if cur2 == cur {
				db.mu.RUnlock()
				return false
			} else {
				cur = cur2
			}
		} else if key > binary.BigEndian.Uint64(db.dbmem[cur*13:cur*13+8]) {
			start = cur
			cur2 := (start + end) / 2
			if cur2 == cur {
				db.mu.RUnlock()
				return false
			} else {
				cur = cur2
			}
		} else {
			db.mu.RUnlock()
			return true
		}
	}
	db.mu.RUnlock()
	return false
}

func (db *QuickDataFile) Get(key uint64) []byte {
	db.mu.RLock()
	start := 0
	end := len(db.dbmem) / 13
	if end == 0 {
		db.mu.RUnlock()
		return nil
	}
	cur := (start + end) / 2
	for true {
		if key < binary.BigEndian.Uint64(db.dbmem[cur*13:cur*13+8]) {
			end = cur
			cur2 := (start + end) / 2
			if cur2 == cur {
				db.mu.RUnlock()
				return nil
			} else {
				cur = cur2
			}
		} else if key > binary.BigEndian.Uint64(db.dbmem[cur*13:cur*13+8]) {
			start = cur
			cur2 := (start + end) / 2
			if cur2 == cur {
				db.mu.RUnlock()
				return nil
			} else {
				cur = cur2
			}
		} else {
			keybt := make([]byte, 8)
			copy(keybt[3:], db.dbmem[cur*13+8:cur*13+13])
			if binary.BigEndian.Uint64(keybt) == (1<<40)-1 {
				db.mu.RUnlock()
				return []byte{}
			} else {
				db.filemu.Lock()
				_, setpose := db.pathfile.Seek(int64(binary.BigEndian.Uint64(keybt)), os.SEEK_SET)
				if setpose == nil {
					lenbt := make([]byte, 4)
					db.pathfile.Read(lenbt)
					valuebt := make([]byte, binary.BigEndian.Uint32(lenbt))
					db.pathfile.Read(valuebt)
					db.filemu.Unlock()
					db.mu.RUnlock()
					return valuebt
				}
				db.filemu.Unlock()
			}
			db.mu.RUnlock()
			return nil
		}
	}
	db.mu.RUnlock()
	return nil
}

func (db *QuickDataFile) RandGet() (key uint64, value []byte) {
	db.mu.Lock()
	cnt := len(db.dbmem) / 13
	if cnt == 0 {
		db.mu.Unlock()
		return 0, nil
	}
	cur := rand.Uint64() % uint64(cnt)
	keybt := make([]byte, 8)
	copy(keybt[3:], db.dbmem[cur*13+8:cur*13+13])
	if binary.BigEndian.Uint64(keybt) == (1<<40)-1 {
		db.mu.Unlock()
		return key, []byte{}
	} else {
		_, setpose := db.pathfile.Seek(int64(binary.BigEndian.Uint64(keybt)), os.SEEK_SET)
		if setpose == nil {
			db.pathfile.Read(keybt[:4])
			valuebt := make([]byte, binary.BigEndian.Uint32(keybt[:4]))
			db.pathfile.Read(valuebt)
			key := binary.BigEndian.Uint64(db.dbmem[cur*13 : cur*13+8])
			//copy(db.dbmem[cur*13:], db.dbmem[cur*13+13:])
			//db.dbmem = db.dbmem[:len(db.dbmem)-13]
			db.mu.Unlock()
			return key, valuebt
		}
	}
	db.mu.Unlock()
	return 0, nil
}

func (db *QuickDataFile) Delete(key uint64) bool {
	db.mu.Lock()
	start := 0
	end := len(db.dbmem) / 13
	if end == 0 {
		db.mu.Unlock()
		return false
	}
	cur := (start + end) / 2
	for true {
		if key < binary.BigEndian.Uint64(db.dbmem[cur*13:cur*13+8]) {
			end = cur
			cur2 := (start + end) / 2
			if cur2 == cur {
				db.mu.Unlock()
				return false
			} else {
				cur = cur2
			}
		} else if key > binary.BigEndian.Uint64(db.dbmem[cur*13:cur*13+8]) {
			start = cur
			cur2 := (start + end) / 2
			if cur2 == cur {
				db.mu.Unlock()
				return false
			} else {
				cur = cur2
			}
		} else {
			copy(db.dbmem[cur*13:], db.dbmem[cur*13+13:])
			db.dbmem = db.dbmem[:len(db.dbmem)-13]
			db.mu.Unlock()
			return true
		}
	}
	db.mu.Unlock()
	return false
}

func (db *QuickDataFile) Count() uint64 {
	db.mu.RLock()
	end := uint64(len(db.dbmem) / 13)
	db.mu.RUnlock()
	return end
}

func (db *QuickDataFile) Flush() bool {
	ioutil.WriteFile(db.path, db.dbmem, 0666)
	db.pathfile.Sync()
	return true
}

func (db *QuickDataFile) Close() bool {
	ioutil.WriteFile(db.path, db.dbmem, 0666)
	db.pathfile.Close()
	db.dbmem = []byte{}
	db.dbclosed = true
	return true
}

func (db *QuickDataFile) PrintAll() {
	keybt := make([]byte, 8)
	for cur := 0; cur < len(db.dbmem)/13; cur++ {
		copy(keybt[:3], []byte{0, 0, 0})
		copy(keybt[3:], db.dbmem[cur*13+8:cur*13+13])
		if binary.BigEndian.Uint64(keybt) == (1<<40)-1 {
			fmt.Println(binary.BigEndian.Uint64(db.dbmem[cur*13:cur*13+8]), "")
		} else {
			_, setpose := db.pathfile.Seek(int64(binary.BigEndian.Uint64(keybt)), os.SEEK_SET)
			if setpose == nil {
				db.pathfile.Read(keybt[:4])
				valuebt := make([]byte, binary.BigEndian.Uint32(keybt[:4]))
				db.pathfile.Read(valuebt)
				fmt.Println(binary.BigEndian.Uint64(db.dbmem[cur*13:cur*13+8]), string(valuebt))
			} else {
				panic("Seek to position error")
			}
		}
	}
}

func (db *QuickDataFile) Export(txtpath string) {
	ff, _ := os.OpenFile(txtpath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
	keybt := make([]byte, 8)
	for cur := 0; cur < len(db.dbmem)/13; cur++ {
		copy(keybt[:3], []byte{0, 0, 0})
		copy(keybt[3:], db.dbmem[cur*13+8:cur*13+13])
		if binary.BigEndian.Uint64(keybt) == (1<<40)-1 {
			ff.Write([]byte(strconv.FormatUint(binary.BigEndian.Uint64(db.dbmem[cur*13:cur*13+8]), 10)))
			ff.Write([]byte{'\t'})
			ff.Write([]byte{})
			ff.Write([]byte{'\n'})
		} else {
			_, setpose := db.pathfile.Seek(int64(binary.BigEndian.Uint64(keybt)), os.SEEK_SET)
			if setpose == nil {
				db.pathfile.Read(keybt[:4])
				valuebt := make([]byte, binary.BigEndian.Uint32(keybt[:4]))
				db.pathfile.Read(valuebt)
				ff.Write([]byte(strconv.FormatUint(binary.BigEndian.Uint64(db.dbmem[cur*13:cur*13+8]), 10)))
				ff.Write([]byte{'\t'})
				ff.Write(valuebt)
				ff.Write([]byte{'\n'})
			} else {
				panic("Seek to position error")
			}
		}
	}
	ff.Close()
}

func DbRewrite(path string) bool {
	dbmem, dbmeme := ioutil.ReadFile(path)
	if dbmeme == nil {
		pathfile, err := os.OpenFile(path+".data", os.O_RDONLY, 0666)
		if err == nil {
			pathfilenew, err2 := os.OpenFile(path+".data.rewrite", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
			if err2 == nil {
				keybt := make([]byte, 8)
				newpos := 0
				for cur := 0; cur < len(dbmem)/13; cur++ {
					copy(keybt[:3], []byte{0, 0, 0})
					copy(keybt[3:], dbmem[cur*13+8:cur*13+13])
					_, setpose := pathfile.Seek(int64(binary.BigEndian.Uint64(keybt)), os.SEEK_SET)
					if setpose == nil {
						pathfile.Read(keybt[:4])
						valuebt := make([]byte, binary.BigEndian.Uint32(keybt[:4]))
						pathfile.Read(valuebt)
						pathfilenew.Write(keybt[:4])
						pathfilenew.Write(valuebt)
						binary.BigEndian.PutUint64(keybt, uint64(newpos))
						copy(dbmem[cur*13+8:cur*13+13], keybt[3:])
						newpos += 4 + len(valuebt)
					} else {
						panic("Seek to position error")
					}
				}
				os.Rename(path, path+".old")
				ioutil.WriteFile(path, dbmem, 0666)
				pathfilenew.Close()
			}
			pathfile.Close()
			os.Remove(path + ".data")
			os.Remove(path + ".old")
			os.Rename(path+".data.rewrite", path+".data")
		}
	}
	return true
}

func ToOrderFileDataFile(path string) bool {
	dbmem, dbmeme := ioutil.ReadFile(path)
	if dbmeme == nil {
		pathfile, err := os.OpenFile(path+".data", os.O_RDONLY, 0666)
		if err == nil {
			pathfilenew, err2 := os.OpenFile(path+".data.rewrite", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
			if err2 == nil {
				ofdb := orderfilev4sim.OpenOrderFile(path+".ofdb", 4)
				ofdb.SetFixKeyLength(8)
				ofdb.EnableRmpushLog(false)
				ofdb.SetFlushInterval(9999999999)
				for cur := 0; cur < len(dbmem)/13; cur++ {
					ofdb.RealPush(dbmem[cur*13 : cur*13+13])
					if cur%100000 == 0 {
						fmt.Println(cur)
					}
				}
				os.Rename(path, path+".old")
				ofdb.Close()
				orderfilev4sim.Rename(path+".ofdb", path)
				os.Remove(path + ".old")
				pathfilenew.Close()
			}
			pathfile.Close()
			os.Remove(path + ".data.rewrite")
		}
	}
	return true
}

// have repeat hash code
// func Hash64(str []byte, mixls ...uint64) uint64 {
// 	// set 'mix' to some value other than zero if you want a tagged hash
// 	mulp := uint64(2654435789)
// 	var mix uint64
// 	if len(mixls) > 0 {
// 		mix = mixls[0]
// 	}
// 	mix ^= uint64(104395301)
// 	for i := 0; i < len(str); i++ {
// 		mix += (uint64(str[i]) * mulp) ^ (mix >> 23)
// 	}
// 	return mix ^ (mix << 37)
// }

//too slow
// func RSHash(str []byte) uint64 {
// 	b := uint64(378551)
// 	a := uint64(63689)
// 	hash := uint64(0)
// 	for i := 0; i < len(str); i++ {
// 		hash = hash*a + uint64(str[i])
// 		a = a * b
// 	}
// 	return hash
// }

// have repeat hash code
// func JSHash(str []byte) uint64 {
// 	hash := uint64(1315423911)
// 	for i := 0; i < len(str); i++ {
// 		hash ^= ((hash << 5) + uint64(str[i]) + (hash >> 2))
// 	}
// 	return hash
// }

// have repeat hash code
// func ELFHash(str []byte) uint64 {
// 	hash := uint64(0)
// 	for i := 0; i < len(str); i++ {
// 		hash = (hash << 4) + uint64(str[i])
// 		x := hash & 0xF000000000000000
// 		if x != 0 {
// 			hash ^= (x >> 24)
// 		}
// 		hash &= ^x
// 	}
// 	return hash
// }

func BKDRHash(str []byte) uint64 {
	seed := uint64(131313)
	hash := uint64(0)
	for i := 0; i < len(str); i++ {
		hash = (hash * seed) + uint64(str[i])
	}
	return hash
}

func SDBMHash(str []byte) uint64 {
	hash := uint64(0)
	for i := 0; i < len(str); i++ {
		hash = uint64(str[i]) + (hash << 6) + (hash << 16) - hash
	}
	return hash
}

// have repeat hash code
// func DJBHash(str []byte) uint64 {
// 	hash := uint64(5381)
// 	for i := 0; i < len(str); i++ {
// 		hash = ((hash << 5) + hash) + uint64(str[i])
// 	}
// 	return hash
// }

// have repeat hash code
// func DEKHash(str []byte) uint64 {
// 	hash := uint64(len(str))
// 	for i := 0; i < len(str); i++ {
// 		hash = ((hash << 5) ^ (hash >> 27)) ^ uint64(str[i])
// 	}
// 	return hash
// }

//too slow
// func APHash(str []byte) uint64 {
// 	hash := uint64(0xAAAAAAAAAAAAAAAA)
// 	for i := 0; i < len(str); i++ {
// 		if (i & 1) == 0 {
// 			hash ^= ((hash << 7) ^ uint64(str[i])*(hash>>3))
// 		} else {
// 			hash ^= (^((hash << 11) + (uint64(str[i]) ^ (hash >> 5))))
// 		}
// 	}
// 	return hash
// }

func DbClear(path string) bool {
	os.Remove(path)
	os.Remove(path + ".data")
	return true
}
