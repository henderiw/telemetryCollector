package main

import (
	"fmt"
	"runtime"
	"strconv"
	"time"

	_ "github.com/influxdata/influxdb1-client" // this is important because of the bug in go mod
	client "github.com/influxdata/influxdb1-client/v2"
	log "github.com/sirupsen/logrus"
)

const (
	// Timeout waiting to enqueue message. If queues aren't drained in
	// this time (coupled with buf channel to absorb transients), then
	// we're toast.
	metricsInfluxTimeoutEnqueueSeconds  = 2
	metricsInfluxWaitToReconnectSeconds = 2
	//
	// An estimate of fields per point allows us to setup
	// slices with a capacity which minimise reallocation,
	// without over allocating to omuch
	metricsInfluxFieldsPerPointEstimate = 32
)

type metricsInfluxOutputHandler struct {
	influxServer     string
	username         string
	password         string
	database         string
	consistency      string
	retention        string
	workers          int
	lastWorker       int
	router           *dataMsgRouter
	dataChannelDepth int
	metricsfilename  string
}

// metricsInfluxOutputWorker handles (sub)set of events and translates
// them into measurement POSTs to Influxdb, working completely
// autonomously from any other workers if present.
type metricsInfluxOutputWorker struct {
	influxServer        string
	wkid                int
	influxOutputHandler *metricsInfluxOutputHandler
	dataChan            chan dMsg
	metricsfilename     string
	logctx              *log.Entry
}

func (w *metricsInfluxOutputWorker) worker(m *metricsOutput) {
	var errorTag string
	var influxClient client.Client
	var err error
	var bp client.BatchPoints

	tcLogCtxt.WithFields(log.Fields{
		"file":     "outputMetricsInflux.go",
		"function": "worker",
	}).Info("Run worker")

	defer m.shutdownSyncPoint.Done()

	o := w.influxOutputHandler
	// Add the client batch configuration once, and reuse it
	// for every batch we add.
	batchCfg := client.BatchPointsConfig{
		Database:         o.database,
		Precision:        "ms",
		RetentionPolicy:  o.retention,
		WriteConsistency: o.consistency,
	}

	for {

		influxClient, err = client.NewHTTPClient(client.HTTPConfig{
			Addr:     w.influxServer,
			Username: o.username,
			Password: o.password,
		})

		if err != nil {
			// Wait, and come round again
			tcLogCtxt.WithError(err).WithFields(log.Fields{
				"file":     "outputMetricsInflux.go",
				"function": "worker",
			}).Error("connect to influx node failed (retry")
			time.Sleep(
				time.Duration(metricsInfluxWaitToReconnectSeconds) * time.Second)
			continue
		}

		// Ok, we connected. Lets get into the main loop.
		for {
			msg, ok := <-w.dataChan
			if !ok {
				tcLogCtxt.WithFields(log.Fields{
					"file":     "outputMetricsInflux.go",
					"function": "worker",
				}).Info("Closed worker")
				influxClient.Close()
				return
			}

			bp, err = client.NewBatchPoints(batchCfg)
			if err != nil {
				// Break out of the loop and start worker over. We
				// failed to get a new batch.
				errorTag = "failed to create batch point"
				break
			}

			// ADD DATA MEASUREMENT
			// Create a point and add to batch
			data := msg.getDataMsgBody()
			tags := map[string]string{
				"node": data.Node,
				"path": data.Path,
			}

			fields := make(map[string]interface{}, len(data.Updates))
			for u, v := range data.Updates {
				i, err := strconv.ParseInt(v.(string), 10, 64)
				if err != nil {
					panic(err)
				}
				fields[u] = i
			}

			pt, err := client.NewPoint("interface_stats", tags, fields, time.Unix(0, data.Timestamp*int64(time.Nanosecond)))

			bp.AddPoint(pt)

			err = influxClient.Write(bp)
			if err != nil {
				errorTag = "failed to write batch point"
				break
			}
			fmt.Printf("Worker: %d, processed msg to influxDB with tstp: %s \n", w.wkid, time.Unix(0, data.Timestamp*int64(time.Millisecond)))
			fmt.Printf("Worker: %d, tags:\n %#v \n", w.wkid, tags)
			fmt.Printf("Worker: %d, field:\n %#v \n", w.wkid, fields)
		}

		//
		// Close existing client.
		influxClient.Close()

		//
		// It might be too noisy to log error here. We may need to
		// consider dampening and relying on exported metric
		tcLogCtxt.WithError(err).WithFields(log.Fields{
			"file":      "outputMetricsInflux.go",
			"function":  "worker",
			"error_tag": errorTag,
		}).Error("exit loop handling messages, will reconnect")

		time.Sleep(
			time.Duration(metricsInfluxWaitToReconnectSeconds) * time.Second)

	}
}

func (o *metricsInfluxOutputHandler) setupWorkers(m *metricsOutput) {
	//
	// Setup as many workers with their own context as necessary. We
	// also route to the various workers using a dedicated
	// router. This will be generalised.
	//
	// Assuming we are picking up off the pub/sub bus, we could be
	// splitting the load by kafka partition already, and this
	// might be one instance of a group of pipelines operating as
	// a consumer group.
	//
	var dumpfilename string

	tcLogCtxt.WithFields(
		log.Fields{
			"metric export":    "influx",
			"InfluxDB server":  o.influxServer,
			"database":         o.database,
			"consistency":      o.consistency,
			"retention":        o.retention,
			"workers":          o.workers,
			"dataChannelDepth": o.dataChannelDepth,
		}).Info("influxDB setup workers")

	o.router = &dataMsgRouter{
		dataChanIn:   m.dChan,
		shutdownChan: m.shutdownChan,
		dataChansOut: make([]chan dMsg, o.workers),
		route: func(msg dMsg, attempts int) int {
			// simple round robin algorithm.
			o.lastWorker++
			o.lastWorker %= o.workers
			return o.lastWorker
		},
		handleCongested: func(
			msg dMsg, attempts int, worker int) dataMsgRouterCongestionAction {

			return datamsgRouterDrop
		},
		// We do not really use the timeout. Behaviour is currently to
		// hunt for worker whcih can take message or drop.
		timeout: time.Duration(metricsInfluxTimeoutEnqueueSeconds) * time.Second,
	}

	//
	// Inherit channel depth for workers too.
	o.dataChannelDepth = m.dataChannelDepth
	for i := 0; i < o.workers; i++ {

		o.router.dataChansOut[i] = make(chan dMsg, o.dataChannelDepth)

		m.shutdownSyncPoint.Add(1)

		w := &metricsInfluxOutputWorker{
			influxServer:        o.influxServer,
			wkid:                i,
			influxOutputHandler: o,
			dataChan:            o.router.dataChansOut[i],
			metricsfilename:     dumpfilename,
		}

		go w.worker(m)
	}

	//
	// Kick off router to start collecting and routing messages to
	// workers.
	go o.router.run()
}

// Process the configuration to extract whatever is needed.
func metricsInfluxNew(name string, ec entityConfig) (metricsOutputHandler, error) {
	var err error

	influxServer, _ := ec.config.GetString(name, "influxDBServer")
	if influxServer == "" {
		err = fmt.Errorf(
			"attribute 'influxDBServer' required for influx metric export. " +
				"Specify URL of the form " +
				"http://[ipv6-host%%zone]:port or " +
				"http://influx.example.com:port.")

		tcLogCtxt.WithError(err).WithFields(
			log.Fields{
				"name":          name,
				"metric export": "influx",
			}).Error("influxDBServer required in config file")

		return nil, err
	}

	username, _ := ec.config.GetString(name, "username")
	if username == "" {
		tcLogCtxt.WithError(err).WithFields(
			log.Fields{
				"name":          name,
				"metric export": "influx",
			}).Error("username required in config file")
		return nil, err
	}

	password, _ := ec.config.GetString(name, "password")
	if password == "" {
		tcLogCtxt.WithFields(
			log.Fields{
				"name":          name,
				"metric export": "influx",
			}).Error("password required in config file")
		return nil, err
	}

	database, err := ec.config.GetString(name, "database")
	if err != nil {
		err = fmt.Errorf(
			"attribute 'database' required for influx metric export. " +
				" For a database created with 'CREATE DATABASE <name>', " +
				"this setting would be 'database=<name>'")

		tcLogCtxt.WithError(err).WithFields(
			log.Fields{
				"name":          name,
				"metric export": "influx",
			}).Error("database required in config file")

		return nil, err
	}

	consistency, _ := ec.config.GetString(name, "consistency")
	retention, _ := ec.config.GetString(name, "retention")

	workers, _ := ec.config.GetInt(name, "workers")
	if workers == 0 {
		workers = 1
	} else if workers > runtime.GOMAXPROCS(-1) {
		workers = runtime.GOMAXPROCS(-1)
	}

	dataChannelDepth, _ := ec.config.GetInt(name, "dataChannelDepth")

	tcLogCtxt.WithFields(
		log.Fields{
			"name":            name,
			"metric export":   "influx",
			"InfluxDB server": influxServer,
			"database":        database,
			"consistency":     consistency,
			"retention":       retention,
			"workers":         workers,
		}).Info("influxDBServer configuration")

	out := &metricsInfluxOutputHandler{
		influxServer:     influxServer,
		username:         username,
		password:         password,
		database:         database,
		consistency:      consistency,
		retention:        retention,
		workers:          workers,
		dataChannelDepth: dataChannelDepth,
		metricsfilename:  "",
	}

	return out, nil
}
