package main

import (
	"fmt"
	"github.com/Livepeer-Open-Pool/openpool-manager/internal"
	pool "github.com/Livepeer-Open-Pool/openpool-plugin"
	"github.com/Livepeer-Open-Pool/openpool-plugin/config"
	"github.com/Livepeer-Open-Pool/openpool-plugin/models"
	log "github.com/sirupsen/logrus"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"time"
)

// SqliteStoragePlugin provides shared storage.
type SqliteStoragePlugin struct {
	db     *gorm.DB
	config *config.Config
	logger *log.Entry
}

// Ensure StoragePlugin implements pool.StorageInterface ✅
var _ pool.StorageInterface = &SqliteStoragePlugin{}

// NewSqliteStoragePlugin returns a new NewSqliteStoragePlugin instance.
func NewSqliteStoragePlugin() pool.StorageInterface {
	return &SqliteStoragePlugin{}
}

func (s *SqliteStoragePlugin) Init(config *config.Config) {
	// Create a structured logger for this plugin
	s.logger = log.WithFields(log.Fields{
		"component":   "SqliteStoragePlugin",
		"storageFile": config.DataStorageFilePath,
	})
	s.logger.Info("Initializing SqliteStoragePlugin")

	gormDb, err := gorm.Open(sqlite.Open(config.DataStorageFilePath), &gorm.Config{})
	if err != nil {
		s.logger.WithError(err).Fatal("Failed to connect to sqlite storage")
	}

	// AutoMigrate or any other DB initialization here.
	err = gormDb.AutoMigrate(&internal.RemoteWorker{}, &internal.EventLog{}, &internal.PoolPayout{})
	if err := gormDb.AutoMigrate(&internal.RemoteWorker{}, &internal.EventLog{}, &internal.PoolPayout{}); err != nil {
		s.logger.WithError(err).Fatal("Failed to migrate database schema")
	}
	s.db = gormDb
	s.config = config

	s.logger.Info("SqliteStoragePlugin initialized successfully")
}

// AddEvent stores an event.
func (s *SqliteStoragePlugin) AddEvent(event models.PoolEvent) error {
	s.logger.WithFields(log.Fields{
		"eventType": event.GetType(),
		"timestamp": event.GetTimestamp(),
	}).Debug("Adding event to storage")

	err := s.db.Create(&internal.EventLog{
		Type:      event.GetType(),
		Data:      event.GetData(),
		CreatedAt: event.GetTimestamp(),
	}).Error

	if err != nil {
		s.logger.WithError(err).Error("Failed to add event")
	}
	return err
}
func (s *SqliteStoragePlugin) GetLastEventTimestamp() (time.Time, error) {
	s.logger.Debug("Retrieving last event timestamp")
	var maxUnixTime int64

	err := s.db.Model(&internal.EventLog{}).Select("MAX(created_at)").Where("created_at is not null").Scan(&maxUnixTime).Error
	if err != nil {
		s.logger.WithError(err).Error("Failed to fetch last event timestamp")
		return time.Time{}, err
	}

	if maxUnixTime == 0 {
		s.logger.Debug("No events found; returning zero time")
		return time.Time{}, nil
	}

	lastTime := time.Unix(maxUnixTime, 0)
	s.logger.WithField("lastEventTime", lastTime).Debug("Fetched last event timestamp")
	return lastTime, nil
}
func (s *SqliteStoragePlugin) GetPreferredWorkers(criteria models.PreferredWorkerCriteria) ([]models.Worker, error) {
	s.logger.WithFields(log.Fields{
		"Criteria": criteria,
	}).Debug("Retrieving preferred workers")

	//TODO: Basic check only makes sure they are online, need to add performance based checks here.... NEED iNPUT CRITIERA TOO
	var remoteWorkers []*internal.RemoteWorker
	if err := s.db.Find(&remoteWorkers).Where("is_connected = ?", true).Error; err != nil {
		s.logger.WithError(err).Error("Failed to fetch preferred workers")
		return nil, err
	}

	workers := make([]models.Worker, len(remoteWorkers))
	for i, rw := range remoteWorkers {
		workers[i] = rw
	}
	return workers, nil
}

func (s *SqliteStoragePlugin) GetWorkers() ([]models.Worker, error) {
	s.logger.Debug("Retrieving all workers")

	var remoteWorkers []*internal.RemoteWorker
	if err := s.db.Find(&remoteWorkers).Error; err != nil {
		s.logger.WithError(err).Error("Failed to fetch workers")
		return nil, err
	}

	workers := make([]models.Worker, len(remoteWorkers))
	for i, rw := range remoteWorkers {
		workers[i] = rw
	}
	return workers, nil
}

func (s *SqliteStoragePlugin) UpdateWorkerStatus(ethAddress string, connected bool, region string, nodeType string) error {
	s.logger.WithFields(log.Fields{
		"ethAddress": ethAddress,
		"connected":  connected,
		"region":     region,
		"nodeType":   nodeType,
	}).Debug("Updating worker status")

	result := s.db.Model(&internal.RemoteWorker{}).
		Where("eth_address = ? AND region = ? AND node_type = ?", ethAddress, region, nodeType).
		Updates(map[string]interface{}{
			"is_connected": connected,
		})
	if result.Error != nil {
		s.logger.WithError(result.Error).Error("Failed to update worker status")

		return result.Error
	}
	// If no matching record was found, create a new one with the connection info
	if result.RowsAffected == 0 {
		s.logger.Debug("No matching worker found; creating a new record")
		worker := internal.RemoteWorker{
			EthAddress:  ethAddress,
			Region:      region,
			NodeType:    nodeType,
			IsConnected: connected,
		}
		if err := s.db.Create(&worker).Error; err != nil {
			s.logger.WithError(err).Error("Failed to create new worker record")
			return err
		}
	}
	return nil
}

//	func (s *SqliteStoragePlugin) UpdateWorkerJobDetails(jobDetails models.WorkerJobDetails) error {
//		ethAddress := jobDetails.GetWorkerID()
//		region := jobDetails.GetRegion()
//		nodeType := jobDetails.GetNodeType()
//		responseTime := jobDetails.GetResponseTime()
//		s.logger.WithFields(log.Fields{
//			"workerID": ethAddress,
//			"region":   region,
//			"nodeType": nodeType,
//		}).Debug("Creating worker job details")
//
//		if ext, ok := jobDetails.(internal.WorkerJobDetails); ok {
//			s.logger.WithFields(log.Fields{
//				"workerID": ethAddress,
//				"model":    ext.Model,
//				"pipeline": ext.Pipeline,
//				"warm":     ext.Warm,
//			}).Debug("Found AI worker job details; updating AI job details")
//
//			aiDetails := internal.WorkerJobDetails{
//				EthAddress:   ethAddress,
//				NodeType:     nodeType,
//				Region:       region,
//				Model:        ext.Model,
//				Pipeline:     ext.Pipeline,
//				Warm:         ext.Warm,
//				ResponseTime: responseTime,
//			}
//			if err := s.db.Create(&aiDetails).Error; err != nil {
//				s.logger.WithError(err).Error("Failed to create new ai worker job details")
//				return err
//			}
//		} else {
//			if err := s.db.Create(&jobDetails).Error; err != nil {
//				s.logger.WithError(err).Error("Failed to create new worker job details")
//				return err
//			}
//		}
//
//		return nil
//	}
func (s *SqliteStoragePlugin) ResetWorkersOnlineStatus(region string, nodeType string) error {
	s.logger.WithFields(log.Fields{
		"region":   region,
		"nodeType": nodeType,
	}).Info("Resetting worker online status to false")

	result := s.db.Model(&internal.RemoteWorker{}).
		Where("region = ? AND node_type = ?", region, nodeType).
		Update("is_connected", false)
	if result.Error != nil {
		s.logger.WithError(result.Error).Error("Failed to reset workers' online status")
	}
	return result.Error
}

func (s *SqliteStoragePlugin) AddPendingFees(ethAddress string, amount int64, region string, nodeType string) error {
	s.logger.WithFields(log.Fields{
		"ethAddress": ethAddress,
		"region":     region,
		"nodeType":   nodeType,
		"amount":     amount,
	}).Debug("Adding pending fees to worker")

	result := s.db.Model(&internal.RemoteWorker{}).
		Where("eth_address = ? AND region = ? AND node_type = ?", ethAddress, region, nodeType).
		Update("pending_fees", gorm.Expr("pending_fees + ?", amount))
	if result.Error != nil {
		s.logger.WithError(result.Error).Error("Failed to add pending fees")
		return result.Error
	}
	if result.RowsAffected == 0 {
		s.logger.Debug("No matching worker found; creating a new record with initial pending fees")

		worker := internal.RemoteWorker{
			EthAddress:  ethAddress,
			Region:      region,
			NodeType:    nodeType,
			IsConnected: false,
			PendingFees: amount,
		}
		if err := s.db.Create(&worker).Error; err != nil {
			s.logger.WithError(err).Error("Failed to create new worker record for pending fees")
			return err
		}
	}
	return nil
}

// AddPaidFees stores a payout.
func (s *SqliteStoragePlugin) AddPaidFees(ethAddress string, amount int64, txHash string, region string, nodeType string) error {
	s.logger.WithFields(log.Fields{
		"ethAddress": ethAddress,
		"region":     region,
		"nodeType":   nodeType,
		"amount":     amount,
		"txHash":     txHash,
	}).Info("Recording paid fees")

	return s.db.Transaction(func(tx *gorm.DB) error {
		result := s.db.Model(&internal.RemoteWorker{}).
			Where("eth_address = ? AND region = ? AND node_type = ?", ethAddress, region, nodeType).
			Update("paid_fees", gorm.Expr("paid_fees + ?", amount)).
			Update("pending_fees", gorm.Expr("pending_fees - ?", amount))
		if result.Error != nil {
			s.logger.WithError(result.Error).Error("Failed to update paid/pending fees")
			return result.Error
		}
		if result.RowsAffected == 0 {
			msg := fmt.Sprintf("No matching remote worker found to update paid fees")
			s.logger.Warn(msg)
			return fmt.Errorf(msg)
		}
		// Create the pool payout record.
		payout := &internal.PoolPayout{
			EthAddress: ethAddress,
			TxHash:     txHash,
			Fees:       amount,
		}
		if err := s.db.Create(payout).Error; err != nil {
			s.logger.WithError(err).Error("Failed to create pool payout record")
			return err
		} else {
			s.logger.WithFields(log.Fields{
				"ethAddress": ethAddress,
				"txHash":     txHash,
				"amount":     amount,
			}).Info("Pool payout record created successfully")
		}

		return nil
	})
}

func (s *SqliteStoragePlugin) GetPendingFees() (float64, error) {
	s.logger.Debug("Retrieving total pending fees")

	var totalPendingFees int64
	err := s.db.Model(&internal.RemoteWorker{}).Select("SUM(pending_fees)").Scan(&totalPendingFees).Error
	if err != nil {
		s.logger.WithError(err).Error("Failed to fetch total pending fees")
		return 0, err
	}

	// Convert wei to ether
	ether := float64(totalPendingFees) / 1e18
	s.logger.WithField("totalPendingETH", ether).Debug("Computed total pending fees in ETH")
	return ether, nil
}

func (s *SqliteStoragePlugin) GetPaidFees() (float64, error) {
	s.logger.Debug("Retrieving total paid fees")
	var totalPaidFees int64
	err := s.db.Model(&internal.RemoteWorker{}).Select("SUM(paid_fees)").Scan(&totalPaidFees).Error
	if err != nil {
		s.logger.WithError(err).Error("Failed to fetch total paid fees")

		return 0, err
	}

	// Convert wei to ether
	ether := float64(totalPaidFees) / 1e18
	s.logger.WithField("totalPaidETH", ether).Debug("Computed total paid fees in ETH")

	return ether, nil
}

// Exported symbol for plugin loading
var PluginInstance SqliteStoragePlugin
