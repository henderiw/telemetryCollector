package main

import (
	"bufio"
	"bytes"
	"fmt"
	"hash/fnv"
	"net/http"
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

func (p *metricsPrometheusOutputHandler) flushMetric(
	tags []metricsAtom,
	ts uint64,
	context metricsOutputContext) {

	var pushURL string

	buf := context.(*bytes.Buffer)

	if buf.Len() == 0 {
		tcLogCtxt.WithFields(
			log.Fields{
				"pushGWAddress": p.pushGWAddress,
				"jobName":       p.jobName,
				"pushURL":       p.pushURL,
				"instance":      p.instance,
				"tag":           tags,
			}).Debug("metrics export: no metrics in msg")
		return
	}

	//
	// Add unique instance to disambiguate time series for grouped
	// metrics.
	//
	// As multiple metrics make it into the push gateway they are
	// grouped by (job, instance or URL labels). Only one update per
	// group of metrics is tracked at any one time - any subsequent
	// updates to the same group prior to scraping replaces any
	// previous updates.
	//
	// In order to avoid losing updates, we make sure that every
	// unique timeseries as identified by the tags, results in a
	// distinct sequence (module hash collisions).
	p.instanceBuf.Reset()
	for i := 0; i < len(tags); i++ {
		p.instanceBuf.WriteString(
			fmt.Sprintf("%s=\"%v\" ", tags[i].key, tags[i].val))
	}
	if p.instanceBuf.Len() != 0 {
		h := fnv.New64a()
		h.Write(p.instanceBuf.Bytes())
		pushURL = fmt.Sprintf("%s_%v", p.pushURL, h.Sum64())
	} else {
		pushURL = p.pushURL
	}

	//
	// Dump a copy as a string if necessary, showing URL too.
	if p.dump != nil {
		p.dump.WriteString("POST " + pushURL + "\n")
		_, err := p.dump.WriteString(buf.String())
		if err != nil {
			tcLogCtxt.WithError(err).WithFields(
				log.Fields{
					"pushGWAddress": p.pushGWAddress,
					"jobName":       p.jobName,
					"pushURL":       p.pushURL,
					"instance":      p.instance,
				}).Error("failed dump metric to file")
		}
	}

	// POST, (not PUT), to make sure that only metrics in same group
	// are replaced.
	req, err := http.NewRequest("POST", pushURL, buf)
	if err != nil {
		tcLogCtxt.WithError(err).WithFields(
			log.Fields{
				"pushGWAddress": p.pushGWAddress,
				"jobName":       p.jobName,
				"pushURL":       p.pushURL,
				"instance":      p.instance,
			}).Error("http new request")
		return
	}

	req.Header.Set("Content-Type", `text/plain; version=0.0.4`)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		tcLogCtxt.WithError(err).WithFields(
			log.Fields{
				"pushGWAddress": p.pushGWAddress,
				"jobName":       p.jobName,
				"pushURL":       p.pushURL,
				"instance":      p.instance,
			}).Error("http post")
		return
	}

	if resp.StatusCode != 202 {
		err = fmt.Errorf(
			"unexpected status code %d while pushing to %s",
			resp.StatusCode, pushURL)
		tcLogCtxt.WithError(err).WithFields(
			log.Fields{
				"pushGWAddress": p.pushGWAddress,
				"jobName":       p.jobName,
				"pushURL":       p.pushURL,
				"instance":      p.instance,
			}).Error("http reply")
	}

	resp.Body.Close()

	buf.Reset()
}

func (p *metricsPrometheusOutputHandler) buildMetric(
	tags []metricsAtom,
	sensor metricsAtom,
	ts uint64,
	context metricsOutputContext) {
	var delim string

	buf := context.(*bytes.Buffer)

	buf.WriteString(sensor.key)

	if len(tags) > 0 {
		delim = "{"
		for i := 0; i < len(tags); i++ {
			buf.WriteString(
				fmt.Sprintf(
					"%s%s=\"%v\"",
					delim,
					tags[i].key,
					tags[i].val))
			if i == 0 {
				// change delim
				delim = ","
			}
		}
		delim = "} "
	} else {
		delim = " "
	}

	buf.WriteString(fmt.Sprintf("%s%v %v\n", delim, sensor.val, ts))
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
					"name":    m.name,
					"output":  m.output,
					"file":    m.inputSpecFile,
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

			//
			// Make sure we clear any lefto over message (only on
			// error)
			buf.Reset()
			err := msg.produceMetrics(&m.inputSpec, m.outputHandler, buf)
			if err != nil {
				//
				// We should count these and export them from meta
				// monitoring
				tcLogCtxt.WithError(err).WithFields(
					log.Fields{
						"name":    m.name,
						"output":  m.output,
						"file":    m.inputSpecFile,
						"pushURL": p.pushURL,
					}).Error("message producing metrics")
				continue
			}
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
