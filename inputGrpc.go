package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/google/gnxi/utils/xpath"
	pb "github.com/openconfig/gnmi/proto/gnmi"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	// "google.golang.org/grpc/metadata"
)

const (
	grpcWaitToRedial   = 1
	grpcTimeout        = 20000
	grpcEncodeJSON     = 4
	grpcSampleInterval = 10000000000
)

type encoding int

const (
	encodingJSON encoding = 8
)

// called from another file to initialize the input capability structure/type
func grpcInputCapabilityNew() inputCapability {
	return &grpcInputCapability{}
}

// struct that implements the input capability interface
type grpcInputCapability struct {
}

func (cap *grpcInputCapability) initialize(name string, ec entityConfig, dChans []chan<- dMsg) (
	chan<- *cMsg, error) {

	// Set up pseudo random seed for ReqID generation.
	rand.Seed(time.Now().UnixNano())

	// load variables from the config file
	insecure, _ := ec.config.GetBool(name, "insecure")
	tls, _ := ec.config.GetBool(name, "tls")
	tlsCAPem, _ := ec.config.GetString(name, "tlsCAPem")
	tlsClientCertPem, _ := ec.config.GetString(name, "tlsClientCertPem")
	tlsClientKeyPem, _ := ec.config.GetString(name, "tlsClientKeyPem")
	tlsServerName, _ := ec.config.GetString(name, "tlsServerName")
	username, _ := ec.config.GetString(name, "username")
	password, _ := ec.config.GetString(name, "password")
	encodingName, _ := ec.config.GetString(name, "encodingName")
	encap, _ := ec.config.GetString(name, "encap")
	serverSockAddr, err := ec.config.GetString(name, "server")

	tcLogCtxt.WithFields(log.Fields{
		"file":             "grpcInput.go",
		"function":         "init",
		"name":             name,
		"targetSockAddr":   serverSockAddr,
		"username":         username,
		"password":         password,
		"encodingName":     encodingName,
		"encap":            encap,
		"insecure":         insecure,
		"tls":              tls,
		"tlsCAPem":         tlsCAPem,
		"tlsClientCertPem": tlsClientCertPem,
		"tlsClientKeyPem":  tlsClientKeyPem,
		"tlsServerName":    tlsServerName,
	}).Info("Loaded the following variables for initializing the grpc target")

	newSubscriptions := make([]string, 0)
	if err == nil {
		// server info provided, further initialization of the server
		subscriptionCfg, err := ec.config.GetString(name, "subscriptions")
		if err != nil {
			tcLogCtxt.WithError(err).WithFields(
				log.Fields{
					"file":     "grpcInput.go",
					"function": "init",
					"name":     name,
				}).Error("missing subscription in the config file section")

			return nil, err
		}

		subscriptions := strings.Split(subscriptionCfg, ",")
		for _, subscription := range subscriptions {
			newSubscriptions = append(newSubscriptions, strings.TrimSpace(subscription))
		}
		fmt.Printf("Subscriprions: %#v \n", newSubscriptions)

	} else {
		// target information missing in the entity section of the config file
		err = fmt.Errorf("attribute 'target' missing in config file section of the entity")
		tcLogCtxt.WithError(err).WithFields(
			log.Fields{
				"file":     "grpcInput.go",
				"function": "init",
				"name":     name,
			}).Error("Failed to setup gRPC input server")

		return nil, err

	}

	// encoding schema used for grpc telemetry
	streamType := encodingJSON

	// Create a control channel which will be used to control the entity,
	// and kick off the server which will accept connections and
	// listen for control requests.
	cChan := make(chan *cMsg)

	fmt.Printf("SUBSCRIPTIONS: %s\n", newSubscriptions)

	grpcRemoteServer := &grpcRemoteServer{
		name:             name,
		server:           serverSockAddr,
		encap:            streamType,
		encodingName:     encodingName,
		reqID:            rand.Int63(),
		subscriptions:    newSubscriptions,
		username:         username,
		password:         password,
		insecure:         insecure,
		tls:              tls,
		tlsCAPem:         tlsCAPem,
		tlsClientCertPem: tlsClientCertPem,
		tlsClientKeyPem:  tlsClientKeyPem,
		tlsServerName:    tlsServerName,
		cChan:            cChan,
		dChans:           dChans,
		childrenDone:     make(chan struct{}),
	}

	tcLogCtxt.WithFields(log.Fields{
		"file":     "grpcInput.go",
		"function": "init",
	}).Info("GRPC remote server initialized ...")

	go grpcRemoteServer.start()

	tcLogCtxt.WithFields(log.Fields{
		"file":     "grpcInput.go",
		"function": "init",
	}).Info("GRPC remote server run ...")

	return cChan, nil
}

// grpcRemoteServer
type grpcRemoteServer struct {
	name             string
	server           string
	encap            encoding
	encodingName     string
	reqID            int64
	subscriptions    []string
	username         string
	password         string
	insecure         bool
	tls              bool
	tlsCAPem         string
	tlsClientCertPem string
	tlsClientKeyPem  string
	tlsServerName    string
	// Control channel used to control the gRPC client
	cChan <-chan *cMsg
	// Data channels fed by the server
	dChans []chan<- dMsg
	// Use to signal that all children have shutdown.
	childrenDone chan struct{}
	//cancel       context.CancelFunc
}

// loadCertificates loads certificates from file.
func (s *grpcRemoteServer) loadCertificates() ([]tls.Certificate, *x509.CertPool) {
	if s.tlsCAPem == "" || s.tlsClientCertPem == "" || s.tlsClientKeyPem == "" {
		tcLogCtxt.Error("ca and cert and key must be set with file locations")
	}

	certificate, err := tls.LoadX509KeyPair(s.tlsCAPem, s.tlsClientKeyPem)
	if err != nil {
		tcLogCtxt.WithError(err).Error("could not load client key pair")
	}

	certPool := x509.NewCertPool()
	caFile, err := ioutil.ReadFile(s.tlsCAPem)
	if err != nil {
		tcLogCtxt.WithError(err).Error("could not read CA certificate")
	}

	if ok := certPool.AppendCertsFromPEM(caFile); !ok {
		tcLogCtxt.Error("failed to append CA certificate")
	}

	return []tls.Certificate{certificate}, certPool
}

func (s *grpcRemoteServer) String() string {
	return s.name
}

func (s *grpcRemoteServer) GetRequestMetadata(context.Context, ...string) (
	map[string]string, error) {

	tcLogCtxt.WithFields(log.Fields{
		"file":     "grpcInput.go",
		"function": "GetRequestMetadata",
	}).Info("Getting Request MetaData call ...")

	return map[string]string{
		"username": s.username,
		"password": s.password,
	}, err
}

func (s *grpcRemoteServer) RequireTransportSecurity() bool {

	tcLogCtxt.WithFields(log.Fields{
		"file":     "grpcInput.go",
		"function": "RequireTransportSecurity",
	}).Info("Getting Require Transport security call ...")

	return s.tls
}

// continuous loop to stay connected and pulling streams for all the subscriptions.
func (s *grpcRemoteServer) loop(ctx context.Context) {

	tcLogCtxt.WithFields(log.Fields{
		"file":     "grpcInput.go",
		"function": "loop",
	}).Info("Entering Continuous loop ...")

	// Prepare dial options (TLS, user/password, timeout...)
	opts := []grpc.DialOption{}
	opts = append(opts, grpc.WithTimeout(time.Millisecond*time.Duration(grpcTimeout)))
	opts = append(opts, grpc.WithBlock())
	opts = append(opts, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32)))

	if !s.tls {
		opts = append(opts, grpc.WithInsecure())

		tcLogCtxt.WithFields(log.Fields{
			"file":     "grpcInput.go",
			"function": "loop",
		}).Info("Insecure GRPC call ...")
	} else {

		tlsConfig := &tls.Config{}
		fmt.Printf("s.insecure : %#v \n", s.insecure)
		if s.insecure {
			tlsConfig.InsecureSkipVerify = true
			tlsConfig.ServerName = s.server

			tcLogCtxt.WithFields(log.Fields{
				"file":     "grpcInput.go",
				"function": "loop",
			}).Info("TLS encrypted GRPC call - insecure ...")
		} else {
			certificates, certPool := s.loadCertificates()
			tlsConfig.ServerName = s.server
			tlsConfig.Certificates = certificates
			tlsConfig.RootCAs = certPool

			tcLogCtxt.WithFields(log.Fields{
				"file":     "grpcInput.go",
				"function": "loop",
			}).Info("TLS encrypted GRPC call - secure with TLS...")
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	}

	if s.username != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(s))
	}

	/*
		var creds credentials.TransportCredentials
		var err error
		if s.tlsCAPem != "" {
			creds, err = credentials.NewClientTLSFromFile(
				s.tlsCAPem,
				s.tlsServerName)
			if err != nil {
				tcLogCtxt.WithError(err).WithFields(log.Fields{
					"file":     "grpcInput.go",
					"function": "loop",
				}).Error(
					"PEM loading failed, subscriptions aborted")

				return
			}
		} else {
			creds = credentials.NewClientTLSFromCert(
				nil, s.tlsServerName)
		}
		opts = append(opts, grpc.WithTransportCredentials(creds))
	*/

	//fmt.Printf("Ctxt : %#v \n", ctx)
	//fmt.Printf("Server : %#v \n", s.server)
	//fmt.Printf("Opts : %#v \n", opts)
	//fmt.Printf("Username : %#v \n", s.username)
	//fmt.Printf("Password : %#v \n", s.password)
	//fmt.Printf("TLS : %#v \n", s.tls)

	conn, err := grpc.Dial(s.server, opts...)
	if err != nil {
		tcLogCtxt.WithError(err).WithFields(log.Fields{
			"file":     "grpcInput.go",
			"function": "loop",
		}).Error("Dial failed, retrying")

		// We simply close the childrenDone channel to retry.
		close(s.childrenDone)
		return
	}
	defer conn.Close()

	client := pb.NewGNMIClient(conn)

	// Add gRPC overall timeout to the config options array.
	//ctx, s.cancel = context.WithTimeout(context.Background(), time.Second*time.Duration(3600))
	//ctx = metadata.AppendToOutgoingContext(ctx, "username", s.username, "password", s.password)
	//defer s.cancel()

	tcLogCtxt.WithFields(log.Fields{
		"file":     "grpcInput.go",
		"function": "loop",
	}).Info("GRPC Client created and Connected")

	// init variables
	encoding, ok := pb.Encoding_value[s.encodingName]
	if !ok {
		var gnmiEncodingList []string
		for _, name := range pb.Encoding_name {
			gnmiEncodingList = append(gnmiEncodingList, name)
		}
		tcLogCtxt.WithFields(log.Fields{
			"file":     "grpcInput.go",
			"function": "loop",
		}).Error(
			"Supported encodings: ", strings.Join(gnmiEncodingList, ", "))
	}
	codecType := s.encap

	/*
		var pbPathList []*pb.Path
		var pbModelDataList []*pb.ModelData
		for _, xPath := range s.subscriptions {
			pbPath, err := xpath.ToGNMIPath(xPath)
			if err != nil {
				tcLogCtxt.WithError(err).WithFields(log.Fields{
					"xPath": xPath,
				}).Error("error in parsing xpath to gnmi path")
			}
			pbPathList = append(pbPathList, pbPath)
		}
			getRequest := &pb.GetRequest{
				Encoding:  pb.Encoding(encoding),
				Path:      pbPathList,
				UseModels: pbModelDataList,
			}

			fmt.Println("== getRequest:")
			utils.PrintProto(getRequest)

			getResponse, err := client.Get(ctx, getRequest)
			if err != nil {
				tcLogCtxt.WithError(err).Error("Get failed")
			}

			fmt.Println("== getResponse:")
			utils.PrintProto(getResponse)
	*/

	var wg sync.WaitGroup
	for _, sub := range s.subscriptions {
		pbPath, err := xpath.ToGNMIPath(sub)
		fmt.Printf("pbPath : %#v \n", &pbPath)
		if err != nil {
			tcLogCtxt.WithError(err).WithFields(
				log.Fields{
					"subscription": sub,
				}).Error("error in parsing subscription")

		}

		qos := pb.QOSMarking{
			Marking: 20,
		}

		subscriptions := make([]*pb.Subscription, 1)
		subscriptions[0] = &pb.Subscription{
			//Path:           pbPath,
			Mode:           pb.SubscriptionMode_SAMPLE,
			SampleInterval: grpcSampleInterval,
			//SuppressRedundant: subscription.SuppressRedundant,
			//HeartbeatInterval: uint64(subscription.HeartbeatInterval.Duration.Nanoseconds()),
		}
		fmt.Printf("subscriptions : %#v \n", &subscriptions)

		sl := pb.SubscriptionList{
			Prefix:           pbPath,
			Subscription:     subscriptions,
			UseAliases:       false,
			Qos:              &qos,
			Mode:             pb.SubscriptionList_STREAM,
			AllowAggregation: false,
			Encoding:         pb.Encoding(encoding),
		}
		subscribe := pb.SubscribeRequest_Subscribe{
			Subscribe: &sl,
		}

		subscribeRequest := pb.SubscribeRequest{
			Request: &subscribe,
		}

		tcLogCtxt.WithFields(log.Fields{
			"file":     "grpcInput.go",
			"function": "loop",
			"ReqId":    codecType,
			"Encoding": encoding,
			"Subidstr": sub,
		}).Info("subscription to be sent")

		s.reqID = s.reqID + 1

		streamSub, err := client.Subscribe(ctx)
		err = streamSub.Send(&subscribeRequest)

		if err != nil {
			tcLogCtxt.WithError(err).WithFields(
				log.Fields{
					"subscription": sub,
				}).Error("Subscription setup failed")
			continue
		}

		// Subscription setup, kick off go routine to handle
		// the stream. Add child to wait group such that this
		// routine can wait for all its children on the way
		// out.
		wg.Add(1)
		go singleSubscription(
			ctx, s, sub, s.reqID, streamSub, codecType, s.dChans, &wg)
	}

	wg.Wait()
	tcLogCtxt.WithFields(log.Fields{
		"file":     "grpcInput.go",
		"function": "loop",
	}).Info("All subscriptions closed")
	close(s.childrenDone)

	return
}

func (s *grpcRemoteServer) start() {
	var stats msgStats
	var ctx context.Context
	var cancel context.CancelFunc

	go func() {
		// Startup the system.
		close(s.childrenDone)
	}()

	tcLogCtxt.WithFields(log.Fields{
		"file":     "grpcInput.go",
		"function": "start",
	}).Info("gRPC remote server starting ...")

	for {
		select {
		case <-s.childrenDone:
			// If we receive childrenDone signal, we need to retry.
			// Start by making a new channel.
			ctx, cancel = context.WithCancel(context.Background())
			s.childrenDone = make(chan struct{})
			go s.loop(ctx)

			// wait before retry
			time.Sleep(grpcWaitToRedial * time.Second)

		case msg := <-s.cChan:
			switch msg.id {
			case report:
				data, _ := json.Marshal(stats)
				resp := &cMsg{
					id:       ack,
					data:     data,
					respChan: nil,
				}
				msg.respChan <- resp

			case shutdown:
				tcLogCtxt.WithFields(log.Fields{
					"file":     "grpcInput.go",
					"function": "start",
					"cMsg ID":  msg.id,
				}).Info("received shutdown, closing connections")

				// Flag cancellation of binding and
				// its connections and wait for
				// cancellation to complete
				// synchronously.
				if cancel != nil {
					tcLogCtxt.Debug("waiting for children")
					cancel()
					<-s.childrenDone
				} else {
					tcLogCtxt.Debug(
						"NOT waiting for children")
				}

				tcLogCtxt.WithFields(log.Fields{
					"file":     "grpcInput.go",
					"function": "start",
				}).Debug("gRPC server notify, telemetry collector binding is closed")
				resp := &cMsg{
					id:       ack,
					respChan: nil,
				}
				msg.respChan <- resp
				return

			default:
				tcLogCtxt.WithFields(log.Fields{
					"file":     "grpcInput.go",
					"function": "start",
				}).Error(
					"gRPC block loop, unknown ctrl message")
			}
		}
	}
}

//
//  Handle a single subscription. This function is run as an
//  independent go routine and its role is to receive messages in a
//  stream, and dispatch the decoded content onto the downstream
//  channels. Any failures will cause the stream handling to terminate
//  and indicate as much to the wait group. Equally, if the
//  subscription is cancelled we bail out.
func singleSubscription(ctx context.Context, s *grpcRemoteServer, sub string, reqID int64, stream pb.GNMI_SubscribeClient,
	codec encoding, dataChans []chan<- dMsg, wg *sync.WaitGroup) {

	defer wg.Done()

	tcLogCtxt.WithFields(
		log.Fields{
			"file":         "grpcInput.go",
			"function":     "singleSubscription",
			"subscription": sub,
			"reqID":        reqID,
		}).Info("Subscription handler running")

	for {
		subscribeRsp, err := stream.Recv()
		//utils.PrintProto(subscribeRsp)

		select {
		case <-ctx.Done():
			count := 0
			stream.CloseSend()
			for {
				if _, err := stream.Recv(); err != nil {
					tcLogCtxt.WithFields(log.Fields{
						"file":         "grpcInput.go",
						"function":     "singleSubscription",
						"subscription": sub,
						"reqID":        reqID,
						"count":        count,
					}).Info(
						"Drained on cancellation")
					break
				}
				count++
			}
			return
		default:
			if err != nil {
				tcLogCtxt.WithError(err).WithFields(log.Fields{
					"file":         "grpcInput.go",
					"function":     "singleSubscription",
					"subscription": sub,
					"reqID":        reqID,
					"report":       subscribeRsp.GetError(),
				}).Error("Server terminated subscription")
				return
			}

			dMsgs, err := subscribeResponseParsing(subscribeRsp, s.name)
			if err != nil {
				tcLogCtxt.WithError(err).WithFields(log.Fields{
					"file":         "grpcInput.go",
					"function":     "singleSubscription",
					"subscription": sub,
					"reqID":        reqID,
				}).Error("Subscription parsing failed")
			}
			//fmt.Printf("subRspJSON(%s) subscription(%s): %#v \n", s.name, sub, dMsgs)
			//fmt.Printf("subRspJSON(%s) subscription(%s): \n", s.name, sub)
			tcLogCtxt.WithFields(log.Fields{
				"file":         "grpcInput.go",
				"function":     "singleSubscription",
				"subscription": sub,
				"Origin":       s.name,
			}).Info("Subscription parsed")

			if dMsgs != nil {
				for _, dMsg := range dMsgs {
					// Push data onto channel to output modules
					for _, dataChan := range dataChans {
						//
						// Make sure that if
						// we are blocked on
						// consumer, we still
						// handle cancel.
						select {
						case <-ctx.Done():
							return
						case dataChan <- dMsg:
							continue
						}
					}
				}
			}

		}
	}
}

func joinPath(path *pb.Path, info string) string {
	var xpath []string
	var end int
	var start int
	//fmt.Printf("Path Elem length: %d \n", len(path.Elem))
	switch info {
	case "full":
		start = 0
		end = len(path.Elem)
	case "first":
		start = 0
		end = len(path.Elem) - 1
	case "last":
		start = len(path.Elem) - 1
		end = len(path.Elem)
	}
	for i := start; i < end; i++ {
		//fmt.Printf("Elem Name : %d : %s \n", i, path.Elem[i].Name)
		elementString := path.Elem[i].Name
		if path.Elem[i].Key != nil {
			for k, v := range path.Elem[i].Key {
				elementString += "[" + k + "=" + v + "]"
			}
			//fmt.Printf("Elem Name Key : %d : %s \n", i, path.Elem[i].Key)
		}
		xpath = append(xpath, elementString)
	}
	//fmt.Printf("Xpath : %s \n", xpath)
	return strings.Join(xpath, "/")
}

func convertUpdate(update *pb.Update) (interface{}, error) {
	var value interface{}

	valueType := fmt.Sprintf("%T\n", update.Val.GetValue())
	//fmt.Printf("Value Type: %s", valueType)

	if strings.Contains(valueType, "String") {
		value = update.Val.GetStringVal()
		return value, nil
	}
	if strings.Contains(valueType, "Json") {
		decoder := json.NewDecoder(bytes.NewReader(update.Val.GetJsonVal()))
		decoder.UseNumber()
		err := decoder.Decode(&value)
		if err != nil {
			return nil, fmt.Errorf("Malformed JSON update %q in %s",
				update.Value.GetValue(), update)
		}
		return value, nil
	}

	return nil, fmt.Errorf("Unhandled type of value %v in %s", update.Value.GetType(), update)

}

// subscribeResponseParsing converts a SubscribeResponse into a JSON object data Message
//func subscribeResponseParsing(resp *pb.SubscribeResponse) (map[string]interface{}, error) {
func subscribeResponseParsing(resp *pb.SubscribeResponse, origin string) ([]dMsg, error) {
	dMs := make([]dMsg, 1)
	msgData := &dMsgJSON{}
	msgBody := &dMsgBody{}
	msgBody.Node = origin

	switch resp := resp.Response.(type) {
	case *pb.SubscribeResponse_Update:
		//fmt.Println("##############################################")
		//fmt.Println("SubscribeResponse_Update")
		notif := resp.Update
		msgBody.Timestamp = notif.Timestamp
		//fmt.Printf("notif.timestamp : %#v \n", notif.Timestamp)
		//fmt.Printf("notif.Prefix : %#v \n", notif.Prefix)
		if notif.Prefix != nil {
			msgBody.Path = "/" + joinPath(notif.Prefix, "full")
			//fmt.Printf("Path : %s \n", msgBody.Path)
		}
		//fmt.Printf("notif.Update length : %d \n", len(notif.Update))
		if len(notif.Update) != 0 {
			//fmt.Println("##############################################")
			updates := make(map[string]interface{}, len(notif.Update))
			for _, update := range notif.Update {
				//fmt.Println("##############################################")
				//fmt.Printf("Update : %s \n", update)
				//fmt.Printf("Update path : %s \n", joinPath(update.Path, "full"))
				//c, err := convertUpdate(update)
				//if err != nil {
				//	return "", err
				//}
				//fmt.Printf("Update path : %s \n", c)

				if notif.Prefix == nil {
					path1 := joinPath(update.Path, "first")
					path2 := joinPath(update.Path, "last")
					msgBody.Path = path1
					updates[path2], err = convertUpdate(update)
				} else {
					updates[joinPath(update.Path, "full")], err = convertUpdate(update)
				}

			}
			msgBody.Updates = updates

		}

		if len(notif.Delete) != 0 {
			//fmt.Println("##############################################")
			deletes := make([]string, len(notif.Delete))
			for i, del := range notif.Delete {
				deletes[i] = joinPath(del, "full")
			}
			msgBody.Deletes = deletes
		}
		msgData.DMsgType = "notification update"
		msgData.DMsgBody = *msgBody
	case *pb.SubscribeResponse_SyncResponse:
		//fmt.Println("##############################################")
		//fmt.Println("SubscribeResponse_SyncResponse")
		msgData.DMsgType = "syncResponse"
		msgData.DMsgBody.SyncResponse = resp.SyncResponse
	default:
		//fmt.Printf("Response type: %#v \n", resp)
		return dMs, fmt.Errorf("Unknown type of response: %T: %s", resp, resp)
	}
	//js, err := json.MarshalIndent(m, "", "  ")
	//if err != nil {
	//	return "", err
	//}
	//return string(js), nil

	dMs[0] = &dMsgJSON{
		DMsgType:   msgData.DMsgType,
		DMsgBody:   msgData.DMsgBody,
		DMsgOrigin: origin,
	}
	return dMs, nil
}
