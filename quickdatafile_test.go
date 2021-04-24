package quickdatafile

import (
	_ "encoding/binary"
	"orderfilev4sim"
	_ "time"
	_ "toolfunc"

	//"encoding/binary"
	"fmt"
	"testing"
	//"time"
)

func Test1(t *testing.T) {
	DbClear("testBKDRHashdb")
	db := NewQuickDataFile("testBKDRHashdb")

	db.Put(BKDRHash([]byte("111,xv")), []byte("222"))
	db.Put(BKDRHash([]byte("dlskajflkdsancxvnknskrfhldsflkdsankcvnck,xv")), []byte("111"))
	db.Put(BKDRHash([]byte("bbb,xv")), []byte("333"))
	db.Put(BKDRHash([]byte("ddd,xv")), []byte("444"))
	db.Put(BKDRHash([]byte("dddd,xv")), []byte("555"))
	db.Put(BKDRHash([]byte("ccc,xv")), []byte("666"))
	db.Put(BKDRHash([]byte("bhhh,xv")), []byte("777"))
	db.Put(BKDRHash([]byte("aafg,xv")), []byte("888"))
	db.Put(BKDRHash([]byte("jjjh,xv")), []byte("999"))
	db.Put(BKDRHash([]byte("hgvvv,xv")), []byte("10101010"))
	db.Put(BKDRHash([]byte("55gv,xv")), []byte("12121212"))
	db.Put(BKDRHash([]byte("ggy66,xv")), []byte("13131313"))
	db.Put(BKDRHash([]byte("ssdggggfdh,xv")), []byte("141414"))
	db.PrintAll()
	fmt.Println(string(db.Get(BKDRHash([]byte("bbb,xv")))))
	fmt.Println(string(db.Get(BKDRHash([]byte("55gv,xv")))))
	rndkey, rndval := db.RandGet()
	fmt.Println(rndkey, string(rndval))
	db.Delete(BKDRHash([]byte("bbb,xv")))
	fmt.Println(string(db.Get(BKDRHash([]byte("bbb,xv")))))
	fmt.Println(string(db.Get(BKDRHash([]byte("55gv,xv")))))

	fmt.Println(db.Exists(BKDRHash([]byte("bbb,xv"))))
	fmt.Println(db.Exists(BKDRHash([]byte("55gv,xv"))))
	db.Close()
	db = NewQuickDataFile("ccc")
	fmt.Println(db.Exists(BKDRHash([]byte("bbb,xv"))))
	fmt.Println(db.Exists(BKDRHash([]byte("55gv,xv"))))
	fmt.Println(string(db.Get(BKDRHash([]byte("bbb,xv")))))
	fmt.Println(string(db.Get(BKDRHash([]byte("55gv,xv")))))
	db.PrintAll()
	orderfilev4sim.OrderFileClear("abcd")
	//keybt := make([]byte, 13)
	//ts := time.Now().UnixNano()
	// db3 := orderfilev4sim.OpenOrderFile("abcd", 4)
	// db3.SetFlushInterval(3600)
	// db3.SetFixKeyLength(8)
	// db3.EnableRmpushLog(false)
	// for i := 0; i < 100000000; i++ {
	// 	//binary.BigEndian.PutUint64(keybt, uint64(i))
	// 	hkey := SDBMHash(toolfunc.RandPrintChar(24, 60))
	// 	binary.BigEndian.PutUint64(keybt[:8], hkey)
	// 	db3.RealPush(keybt)
	// 	// if hkey == 16 {
	// 	// 	fmt.Println(i, hkey)
	// 	// }
	// 	// if db.Exists(hkey) {
	// 	// 	fmt.Println(i, hkey)
	// 	// 	panic("error")
	// 	// }
	// 	// db.Put(hkey, []byte{})
	// 	if i%100000 == 0 {
	// 		ts2 := time.Now().UnixNano()
	// 		fmt.Println(ts2-ts, db3.Count())
	// 		ts = ts2
	// 		fmt.Println(i)
	// 	}
	// }
	// fmt.Println("end", db3.Count())
	// time.Sleep(10000 * time.Second)
	db.Close()
	// db2 := NewBKDRHashDb(`D:\WordNetHost\allpageurlmapdb`)
	// db2.Export("D:\\keyvalue3.txt")
	// db2.Close()
}
