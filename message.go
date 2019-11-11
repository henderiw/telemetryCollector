package main

type dMsg interface {
	//getDataMsgDescription() string
	//getMetaDataPath() (string, error)
	//getMetaDataIdentifier() (string, error)
	//getMetaData() *dataMsgMetaData
}

//
// Concrete meta data can be returned from message types.
type dMsgMetaData struct {
	Path       string
	Identifier string
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
