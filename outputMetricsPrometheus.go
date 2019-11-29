package main

import (
	"bufio"
	"bytes"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	log "github.com/sirupsen/logrus"
)

type metricsPrometheusOutputHandler struct {
	pushGWAddress string
	jobName       string
	instance      string
	pushURL       string
	instanceBuf   *bytes.Buffer
	//
	// metricsfilename allow for diagnostic dump of metrics as
	// exported.
	metricsfilename string
	dump            *bufio.Writer
}

func (p *metricsPrometheusOutputHandler) worker(m *metricsOutput) {

	//var metricsfile *os.File

	//
	// We don't worry about sizing of buf for this worker. This same
	// buf will be used throughout the life of the worker. Underlying
	// storage will grow with first few message to accomodate largest
	// message built automatically. This knowledge is preserved across
	// message since we only call reset between one message and
	// another. Put another way, buf storage grows monotonically over
	// time.
	buf := new(bytes.Buffer)
	p.instanceBuf = new(bytes.Buffer)

	defer m.shutdownSyncPoint.Done()
	//
	// Start by computing the push URL to use. Using the same approach as
	// push prometheus client.
	if !strings.Contains(p.pushGWAddress, "://") {
		p.pushGWAddress = "http://" + p.pushGWAddress
	}
	if strings.HasSuffix(p.pushGWAddress, "/") {
		p.pushGWAddress = p.pushGWAddress[:len(p.pushGWAddress)-1]
	}
	p.pushURL = fmt.Sprintf(
		"%s/metrics/job/%s/instance/%s",
		p.pushGWAddress,
		url.QueryEscape(p.jobName),
		url.QueryEscape(p.instance))

	/*

		if p.metricsfilename != "" {
			metricsfile, p.dump = metricsSetupDumpfile(
				p.metricsfilename, logCtx)
			if metricsfile != nil {
				defer metricsfile.Close()
			}
		}
	*/

	for {

		select {
		//
		// Look for shutdown
		case <-m.shutdownChan:
			//
			// We're being signalled to leave.
			tcLogCtxt.WithFields(
				log.Fields{
					"name":   m.name,
					"output": m.output,
					//"file":    m.inputSpecFile,
					"pushURL": p.pushURL,
				}).Info("metrics prometheus worker exiting")
			return

		//
		// Receive message
		case msg, ok := <-m.dChan:

			if !ok {
				// Channel has been closed. Our demise is
				// near. SHUTDOWN is likely to be received soon on
				// control channel.
				//
				m.dChan = nil
				continue
			}
			fmt.Printf("Msg: %s \n", msg)

			//
			// Make sure we clear any lefto over message (only on
			// error)
			buf.Reset()
			/*
				err := msg.produceMetrics(&m.inputSpec, m.outputHandler, buf)
				if err != nil {
					//
					// We should count these and export them from meta
					// monitoring
					tcLogCtxt.WithError(err).WithFields(
						log.Fields{
							"name":    m.name,
							"output":  m.output,
							//"file":    m.inputSpecFile,
							"pushURL": p.pushURL,
						}).Error("message producing metrics")
					continue
				}
			*/
		}
	}
}

func (p *metricsPrometheusOutputHandler) setupWorkers(m *metricsOutput) {
	m.shutdownSyncPoint.Add(1)
	go p.worker(m)
}

// Prometheus constrains symbols in sensor name
func (p *metricsPrometheusOutputHandler) adaptSensorName(name string) string {

	re := regexp.MustCompile("[^a-zA-Z0-9_:]+")

	return re.ReplaceAllString(name, "_")
}

//
// Prometheus constrains symbols in tag names
func (p *metricsPrometheusOutputHandler) adaptTagName(name string) string {
	re := regexp.MustCompile("[^a-zA-Z0-9_]+")

	return re.ReplaceAllString(name, "_")
}

func metricsPrometheusNew(name string, ec entityConfig) (metricsOutputHandler, error) {

	var p metricsPrometheusOutputHandler
	var err error

	p.pushGWAddress, err = ec.config.GetString(name, "pushgw")
	if err != nil {
		tcLogCtxt.WithError(err).WithFields(
			log.Fields{
				"name": name,
			}).Error("attribute 'pushgw' required for prometheus metric export")

		return nil, err
	}

	p.jobName, err = ec.config.GetString(name, "jobname")
	if err != nil {
		p.jobName = "telemetry"
	}

	p.instance, err = ec.config.GetString(name, "instance")
	if err != nil {
		p.instance = "telemetryCollector"
	}

	// If not set, will default to false
	//p.metricsfilename, _ = ec.config.GetString(name, "dump")

	return &p, nil
}
