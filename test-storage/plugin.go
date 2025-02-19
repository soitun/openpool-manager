package main

import (
	"fmt"
	"github.com/Livepeer-Open-Pool/openpool-plugin/config"
	"github.com/Livepeer-Open-Pool/openpool-plugin/models"
	"sync"
	"time"

	pool "github.com/Livepeer-Open-Pool/openpool-plugin"
)

// Ensure StoragePlugin implements pool.StorageInterface ✅
var _ pool.StorageInterface = &InMemoryStorage{}

type InMemoryStorage struct {
	mu      sync.RWMutex
	events  []models.PoolEvent
	workers map[string]models.DefaultWorker
	payouts []models.DefaultPayout
}

// NewInMemoryStorage returns a new in-memory storage instance.
func NewInMemoryStorage() pool.StorageInterface {
	return &InMemoryStorage{
		workers: make(map[string]models.DefaultWorker),
	}
}

// Init can be used to initialize configuration if needed.
func (s *InMemoryStorage) Init(cfg *config.Config) {
	// No initialization needed for in-memory storage.
}

// AddEvent stores an event in-memory.
func (s *InMemoryStorage) AddEvent(event models.PoolEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
	return nil
}

// GetLastEventTimestamp returns the latest event timestamp.
func (s *InMemoryStorage) GetLastEventTimestamp() (time.Time, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var maxTimestamp int64
	for _, event := range s.events {
		if event.GetTimestamp() > maxTimestamp {
			maxTimestamp = event.GetTimestamp()
		}
	}

	if maxTimestamp == 0 {
		return time.Time{}, nil
	}
	return time.Unix(maxTimestamp, 0), nil
}

// GetWorkers retrieves all workers stored in-memory.
func (s *InMemoryStorage) GetWorkers() ([]models.Worker, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	workers := make([]models.Worker, 0, len(s.workers))
	for _, worker := range s.workers {
		workers = append(workers, worker)
	}
	return workers, nil
}

// GetPreferredWorkers retrieves online workers stored in-memory.
func (s *InMemoryStorage) GetPreferredWorkers(criteria models.PreferredWorkerCriteria) ([]models.Worker, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	workers := make([]models.Worker, 0, len(s.workers))
	for _, worker := range s.workers {
		if worker.Online {
			workers = append(workers, worker)
		}
	}
	return workers, nil
}

// UpdateWorkerStatus updates or creates a worker record.
func (s *InMemoryStorage) UpdateWorkerStatus(id string, connected bool, region string, nodeType string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	worker, exists := s.workers[id]
	if !exists {
		worker = models.DefaultWorker{
			ID:          id,
			NodeType:    `testing`,
			Region:      "LOCAL",
			PendingFees: 0,
		}
	} else {
		worker.Online = connected
	}
	s.workers[id] = worker
	return nil
}

// ResetWorkersOnlineStatus sets all workers as disconnected for a given region and nodeType.
func (s *InMemoryStorage) ResetWorkersOnlineStatus(region, nodeType string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for key, worker := range s.workers {
		if worker.NodeType == nodeType {
			worker.Online = false
			s.workers[key] = worker
		}
	}
	return nil
}

// AddPendingFees increases the pending fees for a worker.
func (s *InMemoryStorage) AddPendingFees(ethAddress string, amount int64, region, nodeType string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	worker, exists := s.workers[ethAddress]
	if !exists {
		worker = models.DefaultWorker{
			ID:          ethAddress,
			Region:      region,
			NodeType:    nodeType,
			PendingFees: amount,
		}
	} else {
		worker.PendingFees += amount
	}
	s.workers[ethAddress] = worker
	return nil
}

// AddPaidFees records a payout and updates worker balances.
func (s *InMemoryStorage) AddPaidFees(ethAddress string, amount int64, txHash string, region, nodeType string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	worker, exists := s.workers[ethAddress]
	if !exists {
		return fmt.Errorf("failed to find remote worker [%s] to update paid fees", ethAddress)
	}

	worker.PaidFees += amount
	if worker.PendingFees >= amount {
		worker.PendingFees -= amount
	} else {
		worker.PendingFees = 0
	}

	s.workers[ethAddress] = worker

	// Store the payout record
	s.payouts = append(s.payouts, models.DefaultPayout{
		ID:        ethAddress,
		Timestamp: time.Now().Unix(),
		Amount:    amount,
	})

	return nil
}

func (s *InMemoryStorage) GetPendingFees() (float64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var totalPendingFees int64
	for _, worker := range s.workers {
		totalPendingFees += worker.PendingFees
	}

	// Convert wei to ether
	ether := float64(totalPendingFees) / 1e18
	return ether, nil
}

func (s *InMemoryStorage) GetPaidFees() (float64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var totalPaidFees int64
	for _, worker := range s.workers {
		totalPaidFees += worker.PaidFees
	}

	// Convert wei to ether
	ether := float64(totalPaidFees) / 1e18
	return ether, nil
}

// Exported symbol for plugin loading
var PluginInstance InMemoryStorage
