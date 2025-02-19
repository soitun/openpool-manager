package main

import (
	"encoding/json"
	"fmt"
	pool "github.com/Livepeer-Open-Pool/openpool-plugin"
	"github.com/Livepeer-Open-Pool/openpool-plugin/config"
	"github.com/Livepeer-Open-Pool/openpool-plugin/models"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"sync"
	"time"
)

// DataLoaderPlugin handles fetching events from external APIs,
// ordering them, and processing them.
type DataLoaderPlugin struct {
	store          pool.StorageInterface
	apiEndpoints   map[string]string
	nodeTypes      map[string]string
	lastCheck      map[string]time.Time
	receivedJobs   map[string]JobReceived
	poolCommission float64
	fetchInterval  int
	region         string
	mu             sync.Mutex
	logger         *log.Entry
}

// JobReceived represents the payload for "job-received" events.
type JobReceived struct {
	EthAddress string `json:"ethAddress"`
	ModelID    string `json:"modelID"`
	NodeType   string `json:"nodeType"`
	Pipeline   string `json:"pipeline"`
	RequestID  string `json:"requestID"`
	TaskID     int    `json:"taskID"`
}

// JobProcessed represents the payload for "job-processed" events.
type JobProcessed struct {
	ComputeUnits        int    `json:"computeUnits"`
	Fees                int64  `json:"fees"`
	NodeType            string `json:"nodeType"`
	PricePerComputeUnit int    `json:"pricePerComputeUnit"`
	RequestID           string `json:"requestID"`
	ResponseTime        int64  `json:"responseTime"`
	EthAddress          string `json:"ethAddress,omitempty"`
	Pipeline            string `json:"pipeline,omitempty"`
	ModelID             string `json:"modelID,omitempty"`
}

// OrchestratorReset represents an empty payload for "orchestrator-reset" events.
type OrchestratorReset struct{}

// WorkerConnected represents the payload for "worker-connected" events.
type WorkerConnected struct {
	Connection string `json:"connection"`
	EthAddress string `json:"ethAddress"`
	NodeType   string `json:"nodeType"`
}

// WorkerDisconnected represents the payload for "worker-disconnected" events.
type WorkerDisconnected struct {
	EthAddress string `json:"ethAddress"`
	NodeType   string `json:"nodeType"`
}

// Ensure DataLoaderPlugin implements PluginInterface
var _ pool.PluginInterface = (*DataLoaderPlugin)(nil)

// Init initializes the plugin using the provided configuration and storage.
func (p *DataLoaderPlugin) Init(cfg config.Config, store pool.StorageInterface) {
	p.logger = log.WithFields(log.Fields{
		"component": "DataLoaderPlugin",
		"region":    cfg.Region,
	})

	p.logger.Info("Initializing DataLoaderPlugin")

	p.store = store
	p.apiEndpoints = make(map[string]string)
	p.nodeTypes = make(map[string]string)
	p.receivedJobs = make(map[string]JobReceived)
	p.poolCommission = cfg.PoolCommissionRate
	p.region = cfg.Region
	p.fetchInterval = cfg.DataLoaderPluginConfig.FetchIntervalSeconds

	// Initialize API endpoints and node types from config
	for _, source := range cfg.DataLoaderPluginConfig.DataSources {
		p.apiEndpoints[source.NodeType] = source.Endpoint
		p.nodeTypes[source.NodeType] = source.NodeType
	}

	// Initialize lastCheck map
	p.lastCheck = make(map[string]time.Time)
	maxTimestamp, err := p.store.GetLastEventTimestamp()
	if err != nil {
		p.logger.WithError(err).Warn("Error fetching max timestamp from store")
		maxTimestamp = time.Time{}
	}
	p.logger.WithField("maxTimestamp", maxTimestamp).Info("Obtained last event timestamp")

	for nodeType := range p.apiEndpoints {
		p.lastCheck[nodeType] = maxTimestamp
		p.logger.WithFields(log.Fields{
			"nodeType":     nodeType,
			"initialCheck": maxTimestamp,
		}).Debug("Set initial lastCheck time for nodeType")
	}
}

// Start begins the periodic fetch cycle.
func (p *DataLoaderPlugin) Start() {
	p.logger.WithField("fetchInterval", p.fetchInterval).Info("DataLoaderPlugin started")

	interval := time.Duration(p.fetchInterval) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Run the fetch cycle on every tick.
	for range ticker.C {
		p.logger.Debug("Starting fetch cycle")

		var wg sync.WaitGroup
		wg.Add(len(p.apiEndpoints))
		for nodeType, endpoint := range p.apiEndpoints {
			p.logger.WithField("nodeType", nodeType).Debug("Initiating fetchAndStoreEvents in goroutine")
			go p.fetchAndStoreEvents(nodeType, endpoint, &wg)
		}
		// Wait until all fetches for this cycle have finished.
		wg.Wait()
		p.logger.WithTime(time.Now()).Info("Completed fetch cycle")
	}
}

// fetchAndStoreEvents performs the HTTP fetch and processes the events.
func (p *DataLoaderPlugin) fetchAndStoreEvents(nodeType, endpoint string, wg *sync.WaitGroup) {
	defer wg.Done()

	// Safely obtain the last check time.
	p.mu.Lock()
	lastTime := p.lastCheck[nodeType]
	p.mu.Unlock()

	url := fmt.Sprintf("%s?lastCheckTime=%s", endpoint, lastTime.UTC().Format(time.RFC3339))
	fetchLogger := p.logger.WithFields(log.Fields{
		"nodeType": nodeType,
		"endpoint": endpoint,
		"url":      url,
	})
	fetchLogger.Debug("Fetching events from endpoint")
	resp, err := http.Get(url)
	if err != nil {
		fetchLogger.WithError(err).Error("HTTP GET failed")
		return
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fetchLogger.WithError(err).Error("Failed to read response body")
		return
	}

	var rawEvents []struct {
		ID      int    `json:"ID"`
		Payload string `json:"Payload"`
		Version int    `json:"Version"`
		DT      string `json:"DT"`
	}

	if err := json.Unmarshal(body, &rawEvents); err != nil {
		fetchLogger.WithError(err).Error("Failed to parse JSON into rawEvents")
		return
	}

	for _, raw := range rawEvents {
		// Parse the payload to determine the event type.
		var parsedPayload struct {
			EventType string          `json:"event_type"`
			Payload   json.RawMessage `json:"Payload"`
		}

		if err := json.Unmarshal([]byte(raw.Payload), &parsedPayload); err != nil {
			fetchLogger.WithFields(log.Fields{
				"eventID": raw.ID,
			}).WithError(err).Warn("Error parsing payload JSON")
			continue
		}
		parsedTime, err := time.Parse(time.RFC3339, raw.DT)
		if err != nil {
			fetchLogger.WithFields(log.Fields{
				"eventID": raw.ID,
				"rawDT":   raw.DT,
			}).WithError(err).Warn("Error parsing timestamp")
			continue
		}

		p.store.AddEvent(models.DefaultPoolEvent{
			Timestamp: parsedTime.UTC().Unix(),
			Data:      raw.Payload,
			Type:      parsedPayload.EventType,
		})

		switch parsedPayload.EventType {
		case "orchestrator-reset":
			var payload OrchestratorReset
			if err := json.Unmarshal(parsedPayload.Payload, &payload); err != nil {
				fetchLogger.WithFields(log.Fields{
					"eventID": raw.ID,
				}).WithError(err).Warn("Invalid payload for orchestrator-reset event")
				continue
			}

			if err := p.store.ResetWorkersOnlineStatus(p.region, nodeType); err != nil {
				fetchLogger.WithFields(log.Fields{
					"eventID": raw.ID,
				}).WithError(err).Error("Failed to reset Workers Online Status")
				continue
			}
		case "worker-connected":
			var payload WorkerConnected
			if err := json.Unmarshal(parsedPayload.Payload, &payload); err != nil {
				fetchLogger.WithFields(log.Fields{
					"eventID": raw.ID,
				}).WithError(err).Warn("Invalid payload for worker-connected")
				continue
			}
			if err := p.store.UpdateWorkerStatus(payload.EthAddress, true, p.region, nodeType); err != nil {
				fetchLogger.WithFields(log.Fields{
					"eventID":    raw.ID,
					"workerAddr": payload.EthAddress,
				}).WithError(err).Error("Failed to update worker status to online")
				continue
			}

		case "worker-disconnected":
			var payload WorkerDisconnected
			if err := json.Unmarshal(parsedPayload.Payload, &payload); err != nil {
				fetchLogger.WithFields(log.Fields{
					"eventID": raw.ID,
				}).WithError(err).Warn("Invalid payload for worker-disconnected")
				continue
			}
			if err := p.store.UpdateWorkerStatus(payload.EthAddress, false, p.region, nodeType); err != nil {
				fetchLogger.WithFields(log.Fields{
					"eventID":    raw.ID,
					"workerAddr": payload.EthAddress,
				}).WithError(err).Error("Failed to update worker status to offline")
				continue
			}
		case "job-received":
			var payload JobReceived
			if err := json.Unmarshal(parsedPayload.Payload, &payload); err != nil {
				fetchLogger.WithFields(log.Fields{
					"eventID": raw.ID,
				}).WithError(err).Warn("Invalid payload for job-received")
				continue
			}

			if payload.NodeType == "ai" {
				p.mu.Lock()
				p.receivedJobs[payload.RequestID] = payload
				p.mu.Unlock()
			}

		case "job-processed":
			var payload JobProcessed
			if err := json.Unmarshal(parsedPayload.Payload, &payload); err != nil {
				fetchLogger.WithFields(log.Fields{
					"eventID": raw.ID,
				}).WithError(err).Warn("Invalid payload for job-processed")
				continue
			}

			feeAfterCommission := int64(float64(payload.Fees) * p.poolCommission)

			if payload.NodeType == "ai" {
				p.mu.Lock()
				if received, exists := p.receivedJobs[payload.RequestID]; exists {
					payload.EthAddress = received.EthAddress
					delete(p.receivedJobs, payload.RequestID)
				}
				p.mu.Unlock()

				//// Also capture worker connection details from job-processed data.
				//wcd := internal.WorkerJobDetails{
				//	EthAddress:   payload.EthAddress,
				//	NodeType:     payload.NodeType,
				//	Model:        payload.ModelID,
				//	Pipeline:     payload.Pipeline,
				//	ResponseTime: payload.ResponseTime,
				//	Region:       p.region,
				//}
				//if err := p.store.UpdateWorkerJobDetails(wcd); err != nil {
				//	fetchLogger.WithFields(log.Fields{
				//		"eventID":    raw.ID,
				//		"workerAddr": payload.EthAddress,
				//	}).WithError(err).Error("Failed to update worker connection details from job-processed")
				//}
			}
			if err := p.store.AddPendingFees(payload.EthAddress, feeAfterCommission, p.region, nodeType); err != nil {
				fetchLogger.WithFields(log.Fields{
					"eventID":    raw.ID,
					"workerAddr": payload.EthAddress,
				}).WithError(err).Error("Failed to update worker pending fees")
				continue
			}
		default:
			fetchLogger.WithFields(log.Fields{
				"eventID":   raw.ID,
				"eventType": parsedPayload.EventType,
			}).Warn("Unknown event type")
			continue
		}

		// Update the lastCheck timestamp for this nodeType if this event is newer.
		p.mu.Lock()
		if parsedTime.After(p.lastCheck[nodeType]) {
			p.lastCheck[nodeType] = parsedTime
		}
		p.mu.Unlock()
	}

	fetchLogger.WithField("numFetched", len(rawEvents)).Info("Finished fetching new events")
}

// Exported symbol for plugin loading
var PluginInstance DataLoaderPlugin
