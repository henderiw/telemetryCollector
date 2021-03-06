package main

import "fmt"

type dMsg interface {
	getDataMsgDescription() string
	getDataMsgBody() *dMsgBody
	getDataMsgOrigin() string
}

//
// Concrete meta data can be returned from message types.
type dMsgMetaData struct {
	Path       string
	Identifier string
}

type dMsgJSON struct {
	DMsgType   string
	DMsgBody   dMsgBody
	DMsgOrigin string
}

type dMsgBody struct {
	Timestamp    int64
	Node         string
	Path         string
	Updates      map[string]interface{}
	Deletes      []string
	SyncResponse bool
}

func (d *dMsgJSON) getDataMsgDescription() string {
	//_, id := m.getMetaDataIdentifier()
	return fmt.Sprintf("JSON message [%s]", d.DMsgType)
}

func (d *dMsgJSON) getDataMsgBody() *dMsgBody {
	return &d.DMsgBody
}

func (d *dMsgJSON) getDataMsgOrigin() string {
	return d.DMsgOrigin
}

type msgID int

const (
	// Used to request to shutdown, expects ACK on respChan
	shutdown msgID = iota
	// Request to report back on pipeline node state
	report
	// Acknowledge a request.
	ack
)

// Control message channel type
type cMsg struct {
	id       msgID
	data     []byte
	respChan chan *cMsg
}

type msgStats struct {
	msgsOK  uint64
	msgsNOK uint64
}
