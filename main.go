/*
 cn 12306 数据结构


*/
package main

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

var (
	serverId       int //服务编号	
	baseDate       int = int(time.Date(2012, 1, 0, 0, 0, 0, 0, time.UTC).Unix() / (60 * 60 * 24))
	maxSearchCount int = 10 //查询剩余车票返回的最大值
	maxLength      int = 62 //车次最多经停站数
)

var (
	trains map[int]*Train
	lock   sync.Mutex
)

// 车次+日期
type Train struct {
	snow      *snow        //Id生产器
	lock      sync.RWMutex //锁	
	train     int          //车次
	date      int          //日期	
	data      []uint64     //座位数据
	config    *TrainConfig //配置
	dbAddress string       //redis 的地址
	dbKey     string       //redis hashset的key
	dbConnect interface{}  //redis connection
}

func newTrain(train, date int) *Train {
	config := getTrainConfig(train)
	t := &Train{
		snow:      newSnow(train, serverId),
		train:     train,
		date:      date,
		data:      make([]uint64, config.length),
		config:    config,
		dbKey:     "hashset_" + strconv.Itoa((train<<16)|date),
		dbAddress: "db address",
		dbConnect: "db connection"}
	return t
}

// 车次一些配置数据
type TrainConfig struct {
	length int    //座位数
	_      string //其他
}

func getTrainConfig(train int) *TrainConfig {
	return &TrainConfig{length: maxLength}
}

// 订单
type Order struct {
	id    uint64 //订单编号
	user  int64  //用户编号
	index int    //座位编号
	train int    //车次
	date  int    //日期
	stamp int64  //时间戳
}

func init() {
	serverId = 123
	trains = make(map[int]*Train, 1000)

	fmt.Println("init", "server id", serverId, "baseDate", baseDate)
}

// 获取车次数据
func getTrain(train, date int) (*Train, bool) {
	key := (train << 16) | date
	t, ok := trains[key]
	return t, ok
}

// 一次只查询一个车次，可以很容易的扩展到查询多个车次
func search(train, date int, start, end uint8) (count int) {
	if t, ok := getTrain(train, date); ok {
		return t.search(start, end)
	}
	return 0
}

// 一次只预订一个座位，可以很容易扩展为预订多个
func order(user int64, train, date int, start, end uint8) (order Order, ok bool) {
	if t, ok := getTrain(train, date); ok {
		return t.order(user, start, end)
	}
	return
}

// 定时增加车次数据
func addTrain(train, date int) {
	key := (train << 16) | date

	lock.Lock()
	defer lock.Unlock()

	t := newTrain(train, date)
	trains[key] = t
}

// 查询是否有票
func (t *Train) search(start, end uint8) (count int) {
	data := t.data
	var mask uint64 = (1<<(end-start) - 1) << (start)
	for _, d := range data {
		if d&mask == 0 {
			count++
		}
		if count > maxSearchCount {
			break
		}
	}
	return count
}

// 预订
func (t *Train) order(user int64, start, end uint8) (order Order, ok bool) {

	var mask uint64 = (1<<(end-start) - 1) << (start)
	t.lock.Lock()
	defer t.lock.Unlock()

	data := t.data
	length := t.config.length
	for i := 0; i < length; i++ {
		if data[i]&mask != 0 {
			continue
		}

		//持久化,处理队列
		order = Order{id: t.snow.nextInt(), user: user, index: i, train: t.train, date: t.date, stamp: time.Now().Unix()}
		data[i] = data[i] | mask
		return order, true
	}
	return
}

// 将日期转化为整数
func formatDate(t time.Time) int {
	return int(t.Unix()/(60*60*24)) - baseDate
}

func main() {

	date := formatDate(time.Now())
	for i := 0; i < 1024; i++ {
		addTrain(i, date)
	}

	testSearch()

}

func testSearch() {
	total := 1000 * 1000
	date := formatDate(time.Now())

	start := time.Now()
	var count int
	for i := 0; i < total; i++ {
		count = search(total/1000, date, 3, 17)
	}
	end := time.Now()

	fmt.Println("start", start)
	fmt.Println("end", end)
	fmt.Println("duration", end.Sub(start).Nanoseconds()/1000000)
	fmt.Println("search result", count)
}

// id生成器
type snow struct {
	lock     sync.Mutex
	base     int64
	stamp    int64 //上次生成编号的时间戳，24位，分钟为单位
	train    int   //车次编号，13位 = 1024*8
	server   int   //服务编号，9位 = 512
	sequence int   //上次生成编号，18位 = 1024*256
	mask     int
}

func newSnow(train, server int) *snow {
	snow := &snow{
		train:  train,
		server: server,
		base:   time.Date(2012, 1, 0, 0, 0, 0, 0, time.UTC).Unix() / 60,
		mask:   -1 ^ (-1 << 18)}

	return snow
}

func (snow *snow) nextInt() uint64 {
	snow.lock.Lock()
	defer snow.lock.Unlock()

	ts := time.Now().Unix()/60 - snow.base
	if ts == snow.stamp {
		snow.sequence = (snow.sequence + 1) & snow.mask
		if snow.sequence == 0 {
			panic("error:overflow")
		}
	} else {
		snow.sequence = 0
	}
	snow.stamp = ts
	id := (uint64(snow.stamp) << (18 + 9 + 13)) | (uint64(snow.train) << (9 + 18)) |
		(uint64(snow.server) << 18) | (uint64(snow.sequence))
	return id
}
