package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/gnxi/utils"
	"github.com/google/gnxi/utils/xpath"
	pb "github.com/openconfig/gnmi/proto/gnmi"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

const (
	xportGrpcWaitToRedial = 1
	grpcTimeout           = 10000
	grpcEncodeJSON        = 4
	sampleInterval        = 10000000000
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

func (cap *grpcInputCapability) initialize(
	name string,
	ec entityConfig,
	dChans []chan<- dMsg) (chan<- *cMsg, error) {

	newSubscriptions := make([]string, 0)

	// Set up pseudo random seed for ReqID generation.
	rand.Seed(time.Now().UnixNano())

	// load variables from the config file
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
		"tls":              tls,
		"tlsCAPem":         tlsCAPem,
		"tlsClientCertPem": tlsClientCertPem,
		"tlsClientKeyPem":  tlsClientKeyPem,
		"tlsServerName":    tlsServerName,
	}).Info("Loaded the following variables for initializing the grpc target")

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

		/*
			// Handle user/password
			authCollect := grpcUPCollectorFactory()
			err = authCollect.handleConfig(nc, name, serverSockAddr)
			if err != nil {
				return nil, err
			}
		*/

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

	grpcRemoteServer := &grpcRemoteServer{
		name:             name,
		server:           serverSockAddr,
		encap:            streamType,
		encodingName:     encodingName,
		reqID:            rand.Int63(),
		subscriptions:    newSubscriptions,
		username:         username,
		password:         password,
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

// loginCreds holds the login/password
type loginCreds struct {
	Username   string
	Password   string
	RequireTLS bool
}

// GetRequestMetadata gets the current request metadata
func (l *loginCreds) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{
		"Username": l.Username,
		"password": l.Password,
	}, nil
}

// RequireTransportSecurity indicates whether the credentials requires transport security
func (l *loginCreds) RequireTransportSecurity() bool {
	return l.RequireTLS
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
	cancel       context.CancelFunc
}

func (s *grpcRemoteServer) String() string {
	return s.name
}

func (s *grpcRemoteServer) GetRequestMetadata(
	ctx context.Context, uri ...string) (
	map[string]string, error) {

	tcLogCtxt.WithFields(log.Fields{
		"file":     "grpcInput.go",
		"function": "GetRequestMetadata",
	}).Info("Getting Request MetaData call ...")

	//username, password, err := s.auth.getUP()
	username := s.username
	password := s.password
	return map[string]string{
		"username": username,
		"password": password,
	}, err
}

func (s *grpcRemoteServer) RequireTransportSecurity() bool {
	//
	// to be updated

	tcLogCtxt.WithFields(log.Fields{
		"file":     "grpcInput.go",
		"function": "RequireTransportSecurity",
	}).Info("Getting Require Transport security call ...")

	return false
}

// continuous loop to stay connected and pulling streams for all the subscriptions.
func (s *grpcRemoteServer) loop(ctx context.Context) {

	tcLogCtxt.WithFields(log.Fields{
		"file":     "grpcInput.go",
		"function": "loop",
	}).Info("Entering Continuous loop ...")

	// Prepare dial options (TLS, user/password, timeout...)

	opts := []grpc.DialOption{
		grpc.WithTimeout(time.Millisecond * time.Duration(grpcTimeout)),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32)),
	}
	switch s.tls {
	case false:
		opts = append(opts, grpc.WithInsecure())
		tcLogCtxt.WithFields(log.Fields{
			"file":     "grpcInput.go",
			"function": "loop",
		}).Info("Insecure GRPC call ...")
	default:
		var creds credentials.TransportCredentials
		var err error
		tcLogCtxt.WithFields(log.Fields{
			"file":     "grpcInput.go",
			"function": "loop",
		}).Info("TLS encrypted GRPC call ...")
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
	}
	/*
		opts = append(opts, grpc.WithPerRPCCredentials(&loginCreds{
			Username:   s.username,
			Password:   s.password,
			RequireTLS: s.tls}))
	*/

	// Add gRPC overall timeout to the config options array.
	//ctx, _ = context.WithTimeout(context.Background(), time.Second*time.Duration(10))
	ctx, s.cancel = context.WithCancel(context.Background())
	ctx = metadata.AppendToOutgoingContext(ctx, "username", s.username, "password", s.password)

	fmt.Printf("Ctxt : %#v \n", ctx)
	fmt.Printf("Server : %#v \n", s.server)
	fmt.Printf("Opts : %#v \n", opts)
	fmt.Printf("Username : %#v \n", s.username)
	fmt.Printf("Password : %#v \n", s.password)
	fmt.Printf("TLS : %#v \n", s.tls)

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
			SampleInterval: sampleInterval,
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

		clientSub, err := client.Subscribe(ctx)
		err = clientSub.Send(&subscribeRequest)

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
			ctx, s, sub, s.reqID, clientSub, codecType, s.dChans, &wg)
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
			ctx, s.cancel = context.WithCancel(context.Background())
			s.childrenDone = make(chan struct{})
			go s.loop(ctx)

			// wait before retry
			time.Sleep(
				xportGrpcWaitToRedial * time.Second)

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
				if s.cancel != nil {
					tcLogCtxt.Debug(
						"waiting for children")
					s.cancel()
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
func singleSubscription(
	thisCtx context.Context,
	thisServer *grpcRemoteServer,
	thisSub string,
	thisReqID int64,
	thisStream pb.GNMI_SubscribeClient,
	thisCodec encoding,
	thisDataChans []chan<- dMsg,
	thisWg *sync.WaitGroup) {

	defer thisWg.Done()

	tcLogCtxt.WithFields(
		log.Fields{
			"file":         "grpcInput.go",
			"function":     "singleSubscription",
			"subscription": thisSub,
			"reqID":        thisReqID,
		}).Info("Subscription handler running")

	for {
		subscribeRsp, err := thisStream.Recv()
		utils.PrintProto(subscribeRsp)

		select {
		case <-thisCtx.Done():
			count := 0
			thisStream.CloseSend()
			for {
				if _, err := thisStream.Recv(); err != nil {
					tcLogCtxt.WithFields(log.Fields{
						"file":         "grpcInput.go",
						"function":     "singleSubscription",
						"subscription": thisSub,
						"reqID":        thisReqID,
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
					"subscription": thisSub,
					"reqID":        thisReqID,
					"report":       subscribeRsp.GetError(),
				}).Error("Server terminated subscription")
				return
			}

			subRspJSON, _ := subscribeResponseToJSON(subscribeRsp)
			fmt.Printf("subRspJSON: %s", subRspJSON)

			response, ok := subscribeRsp.Response.(*pb.SubscribeResponse_Update)
			if !ok {
				return
			}
			fmt.Printf("response : %s \n", response)

			timestamp := time.Unix(0, response.Update.Timestamp)
			fmt.Printf("Timestamp : %s \n", timestamp)

			/*


				if len(dMs) == 0 {
					tLogCtxt.WithFields(
						log.Fields{
							"file":         "grpcInput.go",
							"function":     "singleSubscription",
							"subscription": thisSub,
							"reqID":        thisReqID,
						}).Error(
						"Extracting msg from stream came up empty")
				}

				if dMs != nil {
					for _, dM := range dMs {
						//
						// Push data onto channel.
						for _, dataChan := range thisDataChans {
							//
							// Make sure that if
							// we are blocked on
							// consumer, we still
							// handle cancel.
							select {
							case <-thisCtx.Done():
								return
							case dataChan <- dM:
								continue
							}
						}
					}
				}

			*/
		}
	}
}

func joinPath(path *pb.Path) string {
	return strings.Join(path.Element, "/")
}

func convertUpdate(update *pb.Update) (interface{}, error) {
	switch update.Value.Type {
	case pb.Encoding_JSON:
		var value interface{}
		decoder := json.NewDecoder(bytes.NewReader(update.Value.Value))
		decoder.UseNumber()
		if err := decoder.Decode(&value); err != nil {
			return nil, fmt.Errorf("Malformed JSON update %q in %s",
				update.Value.Value, update)
		}
		return value, nil
	case pb.Encoding_BYTES:
		return strconv.Quote(string(update.Value.Value)), nil
	default:
		return nil,
			fmt.Errorf("Unhandled type of value %v in %s", update.Value.Type, update)
	}
}

// subscribeResponseToJSON converts a SubscribeResponse into a JSON string
func subscribeResponseToJSON(resp *pb.SubscribeResponse) (string, error) {
	m := make(map[string]interface{}, 1)
	var err error
	switch resp := resp.Response.(type) {
	case *pb.SubscribeResponse_Update:
		fmt.Println("SubscribeResponse_Update")
		notif := resp.Update
		m["timestamp"] = notif.Timestamp
		fmt.Printf("timestamp : %s \n", m["timestamp"])
		m["path"] = "/" + joinPath(notif.Prefix)
		fmt.Printf("Path : %s \n", m["path"])
		if len(notif.Update) != 0 {
			fmt.Printf("notif.Update length : %d \n", len(notif.Update))
			updates := make(map[string]interface{}, len(notif.Update))
			for _, update := range notif.Update {
				fmt.Printf("Update : %#v \n", update)
				updates[joinPath(update.Path)], err = convertUpdate(update)
				if err != nil {
					return "", err
				}
			}
			m["updates"] = updates
		}
		if len(notif.Delete) != 0 {
			deletes := make([]string, len(notif.Delete))
			for i, del := range notif.Delete {
				deletes[i] = joinPath(del)
			}
			m["deletes"] = deletes
		}
		m = map[string]interface{}{"notification": m}
	case *pb.SubscribeResponse_SyncResponse:
		fmt.Println("SubscribeResponse_SyncResponse")
		m["syncResponse"] = resp.SyncResponse
	default:
		fmt.Printf("Response type: %#v \n", resp)
		return "", fmt.Errorf("Unknown type of response: %T: %s", resp, resp)
	}
	js, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return "", err
	}
	return string(js), nil
}
