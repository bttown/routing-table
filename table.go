package table

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sort"
	"time"
)

const (
	nodeIDBits  = 160
	nodeIDBytes = nodeIDBits / 8
	k           = 8
)

var (
	respTimeout       = 1 * time.Second
	dura2backup       = 1 * time.Minute
	dura2Questionable = 15 * time.Minute
)

type Hash [nodeIDBytes]byte

type Table struct {
	ownid         Hash
	buckets       []*bucket
	questionNodes chan *Contact
	pingTimeout   chan *Contact
	packets       chan *Contact
	eventTimers   map[timeoutEvent]*time.Timer
	closeQ        chan struct{}
	knownContacts map[Hash]*Contact
	transport     Transport
	tdbpath       string
}

type bucket struct {
	LastChanged time.Time
	Entries     []*Contact
	Reserves    []*Contact
}

type Transport interface {
	Ping(*net.UDPAddr) error
}

type contactEvent uint

const (
	invalidEvent contactEvent = iota
	questionableEvent
	pingTimeoutEvent
)

type Contact struct {
	UDPAddr       net.UDPAddr
	NID           Hash
	pingFatalTime int
}

func (c *Contact) String() string {
	return fmt.Sprintf("<Contact ID:%s UDP:%s>", hex.EncodeToString(c.NID[:]), c.UDPAddr.String())
}

// updateLastChangedTime ...
func (b *bucket) updateLastChangedTime() {
	b.LastChanged = time.Now()
}

func (b *bucket) addFront(c *Contact) {
	b.Entries = append(b.Entries, nil)
	copy(b.Entries[1:], b.Entries)
	b.Entries[0] = c
	b.updateLastChangedTime()
}

func (b *bucket) bump(c *Contact) (*Contact, bool) {
	var original *Contact
	for i := range b.Entries {
		if b.Entries[i].NID == c.NID {
			original = b.Entries[i]
			copy(b.Entries[1:], b.Entries[:i])
			b.Entries[0] = c
			b.updateLastChangedTime()
			return original, true
		}
	}

	return nil, false
}

// NewTable makes and return a new routing-table.
func NewTable(ownerID Hash, transport Transport) *Table {
	t := &Table{
		ownid:         ownerID,
		tdbpath:       "dump.tdb",
		transport:     transport,
		buckets:       make([]*bucket, 160),
		closeQ:        make(chan struct{}),
		packets:       make(chan *Contact, 20*k),
		questionNodes: make(chan *Contact, 20*k),
		pingTimeout:   make(chan *Contact, 20*k),
		eventTimers:   make(map[timeoutEvent]*time.Timer),
		knownContacts: make(map[Hash]*Contact),
	}

	for i := range t.buckets {
		t.buckets[i] = &bucket{
			Entries:  make([]*Contact, 0),
			Reserves: make([]*Contact, 0),
		}
	}

	go t.loop()
	log.Println("try to loading routing-table", t.loadFromFile(t.tdbpath))

	return t
}

// Closest ...
func (table *Table) Closest(target Hash, nresults int) *NodesByDistance {
	close := &NodesByDistance{target: target}
	for _, b := range table.buckets {
		for _, n := range b.Entries {
			close.push(n, nresults)
		}
	}
	return close
}

// Stop releases resource and save snapshot.
func (table *Table) Stop() {
	close(table.closeQ)
	table.saveToFile(table.tdbpath)
}

// OwnerID returns owner node id.
func (table *Table) OwnerID() Hash {
	return table.ownid
}

func (table *Table) loadFromFile(filepath string) error {
	f, err := os.Open(filepath)
	if err != nil {
		return err
	}

	data := struct {
		OwnerID Hash
		Buckets []*bucket
	}{}

	err = json.NewDecoder(f).Decode(&data)
	if err != nil {
		return err
	}

	table.ownid = data.OwnerID
	for bktid := range data.Buckets {
		bkt := data.Buckets[bktid]
		if bkt != nil {
			for eid := range bkt.Entries {
				table.add(bkt.Entries[eid])
			}
		}
	}

	return nil
}

func (table *Table) saveToFile(filepath string) error {
	f, err := os.Create(filepath)
	if err != nil {
		return err
	}

	defer f.Close()

	data := make(map[string]interface{})
	data["OwnerID"] = table.ownid
	data["Buckets"] = table.buckets
	return json.NewEncoder(f).Encode(data)
}

func (table *Table) loop() {
	backupTicker := time.NewTimer(dura2backup)
	defer backupTicker.Stop()
	for {
		select {
		case n := <-table.questionNodes:
			log.Println("questionable node", n)
			table.ping(n)
		case n := <-table.pingTimeout:
			log.Println("ping timeout node", n)
			table.handlePingTimeout(n)
		case contact := <-table.packets:
			table.update(contact)
		case <-backupTicker.C:
			log.Println("routing-table backup", table.saveToFile(table.tdbpath))
			backupTicker.Reset(dura2backup)
		}
	}
}

func (table *Table) Update(c *Contact) {
	if len(c.NID) != nodeIDBytes {
		return
	}
	select {
	case <-table.closeQ:
	case table.packets <- c:
	}
}

func (table *Table) checkContact(c *Contact) *Contact {
	if n, ok := table.knownContacts[c.NID]; ok {
		return n
	}

	return c
}

func (table *Table) update(c *Contact) {
	contact := table.checkContact(c)
	table.add(contact)
}

func (table *Table) ping(c *Contact) {
	table.knownContacts[c.NID] = c
	table.transport.Ping(&c.UDPAddr)
	table.registerPingTimeoutEvent(respTimeout, c)
}

func (table *Table) handlePingTimeout(c *Contact) {
	c.pingFatalTime++
	if c.pingFatalTime > 1 {
		log.Println("delete", c)
		table.deleteReplace(c)
		return
	}

	// ping again
	table.ping(c)
}

type timeoutEvent struct {
	contact *Contact
	event   contactEvent
}

func (table *Table) registerPingTimeoutEvent(d time.Duration, c *Contact) {
	event := timeoutEvent{contact: c, event: pingTimeoutEvent}
	table.eventTimers[event] = time.AfterFunc(d, func() {
		select {
		case table.pingTimeout <- c:
		case <-table.closeQ:
		}
	})
}

func (table *Table) registerQuestionableEvent(d time.Duration, c *Contact) {
	event := timeoutEvent{contact: c, event: questionableEvent}
	table.eventTimers[event] = time.AfterFunc(d, func() {
		select {
		case table.questionNodes <- c:
		case <-table.closeQ:
		}
	})
}

func (table *Table) abortTimeoutEvent(c *Contact, evt contactEvent) {
	event := timeoutEvent{contact: c, event: evt}
	timer := table.eventTimers[event]
	if timer != nil {
		timer.Stop()
		delete(table.eventTimers, event)
	}
}

func (table *Table) deleteReplace(c *Contact) {
	bucket := table.buckets[bucketid(table.ownid, c.NID)]
	i := 0
	for i < len(bucket.Entries) {
		if bucket.Entries[i].NID == c.NID {
			bucket.Entries = append(bucket.Entries[:i], bucket.Entries[i+1:]...)
		} else {
			i++
		}
	}

	if len(bucket.Reserves) > 0 {
		ri := len(bucket.Reserves) - 1
		bucket.addFront(bucket.Reserves[ri])
		table.registerQuestionableEvent(dura2Questionable, bucket.Reserves[ri])
		bucket.Reserves = bucket.Reserves[:ri]
	}
}

func (table *Table) add(n *Contact) {
	bktid := bucketid(table.ownid, n.NID)
	if bktid >= len(table.buckets) {
		log.Println("invalid bucket id", bktid, n.NID)
		return
	}
	bkt := table.buckets[bktid]
	original, bumped := bkt.bump(n)
	switch {
	case bumped:
		// reset questionable timer
		table.abortTimeoutEvent(original, pingTimeoutEvent)
		table.abortTimeoutEvent(original, questionableEvent)
		table.registerQuestionableEvent(dura2Questionable, n)
	case len(bkt.Entries) < k:
		bkt.addFront(n)
		table.registerQuestionableEvent(dura2Questionable, n)
	default:
		bkt.Reserves = append(bkt.Reserves, n)
		if len(bkt.Reserves) > k {
			copy(bkt.Reserves, bkt.Reserves[1:])
			bkt.Reserves = bkt.Reserves[:k]
		}
	}
}

// distcmp compares the distances a->target and b->target.
// Returns -1 if a is closer to target, 1 if b is closer to target
// and 0 if they are equal.
func distcmp(target, a, b Hash) int {
	for i := range target {
		da := a[i] ^ target[i]
		db := b[i] ^ target[i]
		if da > db {
			return 1
		} else if da < db {
			return -1
		}
	}
	return 0
}

type NodesByDistance struct {
	entries []*Contact
	target  Hash
}

func (h *NodesByDistance) Entries() []*Contact {
	return h.entries
}

func (h *NodesByDistance) push(c *Contact, maxElems int) {
	ix := sort.Search(len(h.entries), func(i int) bool {
		return distcmp(h.target, h.entries[i].NID, c.NID) > 0
	})
	if len(h.entries) < maxElems {
		h.entries = append(h.entries, c)
	}
	if ix == len(h.entries) {
		// farther away than all nodes we already have.
		// if there was room for it, the node is now the last element.
	} else {
		// slide existing entries down to make room
		// this will overwrite the entry we just appended.
		copy(h.entries[ix+1:], h.entries[ix:])
		h.entries[ix] = c
	}
}

func bucketid(ownerid, nid Hash) int {
	if ownerid == nid {
		return 0
	}
	var i int
	var bite byte
	var bitDiff int
	var v byte
	for i, bite = range ownerid {
		v = bite ^ nid[i]
		switch {
		case v > 0x70:
			bitDiff = 8
			goto calc
		case v > 0x40:
			bitDiff = 7
			goto calc
		case v > 0x20:
			bitDiff = 6
			goto calc
		case v > 0x10:
			bitDiff = 5
			goto calc
		case v > 0x08:
			bitDiff = 4
			goto calc
		case v > 0x04:
			bitDiff = 3
			goto calc
		case v > 0x02:
			bitDiff = 2
			goto calc
		}
	}

calc:

	return i*8 + (8 - bitDiff)
}
