// Copyright 2024 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Package simulator provides a simple standalone simulator for go-ethereum.
package simulator

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/internal/ethapi"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rpc"
)

// newEvent represents the result of a transaction simulation
type newEvent struct {
	Tx             *types.Transaction             `json:"tx"`
	Logs           []*types.Log                   `json:"logs"`
	Success        bool                           `json:"success"`
	Error          string                         `json:"error,omitempty"`
	BlockNumber    uint64                         `json:"blockNumber"`
	BalanceChanges map[common.Address]common.Hash `json:"balanceChanges,omitempty"`
}

// Simulator is a simple standalone simulator with start/stop/status functionality
type Simulator struct {
	running bool
	mu      sync.RWMutex
	stopCh  chan struct{}
	logger  log.Logger

	// Transaction pool subscription
	backend ethapi.Backend
	txSub   event.Subscription
	txCh    chan core.NewTxsEvent

	// Block chain subscription
	blockSub event.Subscription
	blockCh  chan core.ChainEvent

	// Logs subscription
	logsSub event.Subscription
	logsCh  chan []*types.Log

	// Pending state and header management
	pendingState  *state.StateDB
	pendingHeader *types.Header
	stateMu       sync.RWMutex

	// Event system for simulation results
	scope      event.SubscriptionScope
	resultFeed event.Feed
}

// NewSimulator creates a new simulator instance
func NewSimulator(backend ethapi.Backend) *Simulator {
	return &Simulator{
		logger:  log.New("module", "simulator"),
		stopCh:  make(chan struct{}),
		backend: backend,
		txCh:    make(chan core.NewTxsEvent, 100),
		blockCh: make(chan core.ChainEvent, 10),
		logsCh:  make(chan []*types.Log, 50),
	}
}

// Start starts the simulator
func (s *Simulator) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return nil // Already running
	}

	s.running = true
	s.stopCh = make(chan struct{})

	// Initialize pending state from the latest block
	if err := s.initializePendingState(); err != nil {
		s.running = false
		return err
	}

	// Subscribe to new transactions, block events, and logs if backend is available
	if s.backend != nil {
		s.txSub = s.backend.SubscribeNewTxsEvent(s.txCh)
		s.blockSub = s.backend.SubscribeChainEvent(s.blockCh)
		s.logsSub = s.backend.SubscribeLogsEvent(s.logsCh)
	}

	// Start background task
	go s.run()

	s.logger.Info("Simulator started")
	return nil
}

// Stop stops the simulator
func (s *Simulator) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return nil // Already stopped
	}

	s.running = false
	close(s.stopCh)

	// Unsubscribe from transactions, block events, and logs
	if s.txSub != nil {
		s.txSub.Unsubscribe()
	}
	if s.blockSub != nil {
		s.blockSub.Unsubscribe()
	}
	if s.logsSub != nil {
		s.logsSub.Unsubscribe()
	}

	// Clear pending state
	s.stateMu.Lock()
	s.pendingState = nil
	s.pendingHeader = nil
	s.stateMu.Unlock()

	s.logger.Info("Simulator stopped")
	return nil
}

// IsRunning returns true if the simulator is running
func (s *Simulator) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.running
}

// initializePendingState initializes the pending state and header from the latest block
func (s *Simulator) initializePendingState() error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	// Get the latest state and header
	stateDB, header, err := s.backend.StateAndHeaderByNumber(context.Background(), rpc.LatestBlockNumber)
	if err != nil {
		return err
	}

	// Copy is necessary here to create an isolated state for pending operations
	s.pendingState = stateDB.Copy()

	// Create a new header with incremented block number for pending operations
	pendingHeader := *header
	pendingHeader.Number = new(big.Int).Add(header.Number, big.NewInt(1))
	s.pendingHeader = &pendingHeader

	s.logger.Info("Pending state initialized", "blockNumber", header.Number.Uint64(), "pendingBlockNumber", s.pendingHeader.Number.Uint64(), "blockHash", header.Hash().Hex())
	return nil
}

// updatePendingStateFromHeader updates the pending state and header from a header
func (s *Simulator) updatePendingStateFromHeader(header *types.Header) error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	// Get the state for the new block
	stateDB, _, err := s.backend.StateAndHeaderByNumber(context.Background(), rpc.BlockNumber(header.Number.Uint64()))
	if err != nil {
		return err
	}

	// Copy is necessary here to create an isolated state for pending operations
	s.pendingState = stateDB.Copy()

	// Create a new header with incremented block number for pending operations
	pendingHeader := *header
	pendingHeader.Number = new(big.Int).Add(header.Number, big.NewInt(1))
	s.pendingHeader = &pendingHeader

	s.logger.Info("Pending state updated from header", "blockNumber", header.Number.Uint64(), "pendingBlockNumber", s.pendingHeader.Number.Uint64(), "blockHash", header.Hash().Hex())
	return nil
}

// GetPendingState returns a copy of the current pending state and header
func (s *Simulator) GetPendingState() (*state.StateDB, *types.Header) {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()

	if s.pendingState == nil || s.pendingHeader == nil {
		return nil, nil
	}

	// Copy is necessary here because we want to isolate transaction execution
	// from the main pending state, allowing us to apply changes only on success
	return s.pendingState.Copy(), s.pendingHeader
}

// simulateTransaction simulates a transaction on the pending state
func (s *Simulator) simulateTransaction(tx *types.Transaction) *newEvent {
	result := &newEvent{
		Tx:      tx,
		Success: false,
	}

	// Get the pending state and header
	simState, header := s.GetPendingState()
	if simState == nil || header == nil {
		result.Error = "pending state not initialized"
		return result
	}

	// Validate transaction
	if tx == nil {
		result.Error = "transaction is nil"
		return result
	}

	// Set the block number in the result
	result.BlockNumber = header.Number.Uint64()

	// Check for valid chain ID and skip transactions from other networks
	chainID := tx.ChainId()
	if chainID == nil || chainID.Sign() == 0 {
		result.Error = "invalid chain ID: chain ID cannot be zero or nil"
		return result
	}

	// Skip transactions from other networks
	currentChainID := s.backend.ChainConfig().ChainID
	if chainID.Cmp(currentChainID) != 0 {
		result.Error = fmt.Sprintf("skipped: transaction chain ID %s does not match current network chain ID %s", chainID.String(), currentChainID.String())
		return result
	}

	// Skip vanilla transactions (plain ETH transfers)
	if tx.Gas() == params.TxGas {
		result.Error = "skipped: vanilla transaction (plain ETH transfer)"
		return result
	}

	// Convert transaction to message
	msg, err := core.TransactionToMessage(tx, types.LatestSignerForChainID(chainID), header.BaseFee)
	if err != nil {
		result.Error = "failed to convert transaction to message: " + err.Error()
		return result
	}

	// Create a map to store balance changes
	result.BalanceChanges = make(map[common.Address]common.Hash)

	// Create tracing hooks to capture balance changes
	hooks := &tracing.Hooks{
		OnBalanceChange: func(addr common.Address, prev, new *big.Int, reason tracing.BalanceChangeReason) {
			// Store the new balance
			result.BalanceChanges[addr] = common.BigToHash(new)
		},
	}
	simState.SetTxContext(tx.Hash(), 0)
	// Create hooked state with tracing hooks
	hookedState := state.NewHookedState(simState, hooks)
	// Use GetEVM with nil state, then assign the hooked state
	evm := s.backend.GetEVM(context.Background(), nil, header, &vm.Config{}, nil)
	evm.StateDB = hookedState

	// Execute the transaction
	gasPool := new(core.GasPool).AddGas(tx.Gas())
	_, err = core.ApplyMessage(evm, msg, gasPool)

	if err != nil {
		result.Error = "transaction execution failed: " + err.Error()
		return result
	}

	// Update the pending state with the changes from this transaction
	// Note: simState is already a copy from getPendingState(), so we can use it directly
	s.stateMu.Lock()
	s.pendingState = simState
	s.stateMu.Unlock()

	result.Success = true
	result.Logs = simState.GetLogs(tx.Hash(), header.Number.Uint64(), header.Hash(), header.Time)
	return result
}

// run is the main loop of the simulator
func (s *Simulator) run() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.logger.Info("Simulator heartbeat", "timestamp", time.Now())
		case ev := <-s.txCh:
			s.logger.Info("New transaction received", "count", len(ev.Txs))
			for _, tx := range ev.Txs {
				s.logger.Info("Simulating transaction", "hash", tx.Hash().Hex())

				// Simulate the transaction
				result := s.simulateTransaction(tx)

				// Log the result
				if result.Success {
					s.logger.Info("Transaction simulation successful",
						"hash", tx.Hash().Hex(),
						"logs", len(result.Logs),
						"balanceChanges", len(result.BalanceChanges))

					// Log balance changes if any
					for _, change := range result.BalanceChanges {
						s.logger.Info("Balance change",
							"address", change.Hex(),
							"balance", change)
					}

					// Emit the simulation result
					s.logger.Info("Sending result to event feed", "hash", tx.Hash().Hex())
					s.resultFeed.Send(result)
					s.logger.Info("Result sent to event feed", "hash", tx.Hash().Hex())
					// } else {
					// 	s.logger.Warn("Transaction simulation failed",
					// 		"hash", tx.Hash().Hex(),
					// 		"error", result.Error)
				}
			}
		case ev := <-s.blockCh:
			s.logger.Info("New block imported", "blockNumber", ev.Header.Number.Uint64(), "blockHash", ev.Header.Hash().Hex())

			// Update pending state with the new block
			if err := s.updatePendingStateFromHeader(ev.Header); err != nil {
				s.logger.Error("Failed to update pending state", "error", err)
			}
		case logs := <-s.logsCh:
			s.logger.Info("New logs received", "count", len(logs), "blockNumber", logs[0].BlockNumber)

			// Create a consolidated newEvent with all logs from the block
			if len(logs) > 0 {
				blockNumber := logs[0].BlockNumber
				consolidatedEvent := &newEvent{
					Logs:        logs,
					Success:     true,
					BlockNumber: blockNumber,
				}
				s.logger.Info("Sending consolidated logs event", "blockNumber", blockNumber, "logCount", len(logs))
				s.resultFeed.Send(consolidatedEvent)
				s.logger.Info("Consolidated logs event sent", "blockNumber", blockNumber)
			}
		}
	}
}

// APIs returns the RPC APIs for the simulator
func (s *Simulator) APIs() []rpc.API {
	return []rpc.API{
		{
			Namespace: "simulator",
			Service:   &SimulatorAPI{simulator: s},
		},
	}
}

// SimulatorAPI provides RPC methods for the simulator
type SimulatorAPI struct {
	simulator *Simulator
}

// Start starts the simulator
func (api *SimulatorAPI) Start() error {
	return api.simulator.Start()
}

// Stop stops the simulator
func (api *SimulatorAPI) Stop() error {
	return api.simulator.Stop()
}

// Status returns the status of the simulator
func (api *SimulatorAPI) Status() map[string]interface{} {
	status := map[string]any{
		"running":   api.simulator.IsRunning(),
		"timestamp": time.Now(),
	}

	// Add pending state information if available
	if api.simulator.pendingHeader != nil {
		status["pendingBlockNumber"] = api.simulator.pendingHeader.Number.Uint64()
		status["pendingStateInitialized"] = true
	} else {
		status["pendingStateInitialized"] = false
	}

	return status
}

// SubscribeSimulationResults subscribes to simulation results
func (api *SimulatorAPI) NewEvents(ctx context.Context) (*rpc.Subscription, error) {
	api.simulator.logger.Info("SubscribeSimulationResults called", "ctx", ctx)

	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		api.simulator.logger.Error("Notifications not supported in context")
		return nil, rpc.ErrNotificationsUnsupported
	}

	subscription := notifier.CreateSubscription()
	api.simulator.logger.Info("Created subscription", "id", subscription.ID)

	go func() {
		results := make(chan *newEvent, 128)
		sub := api.simulator.resultFeed.Subscribe(results)
		defer sub.Unsubscribe()

		api.simulator.logger.Info("Started subscription goroutine", "subscriptionID", subscription.ID)

		for {
			select {
			case result := <-results:
				api.simulator.logger.Info("Sending simulation result to subscriber",
					"subscriptionID", subscription.ID,
					"txHash", result.Tx.Hash().Hex(),
					"success", result.Success,
					"logs", len(result.Logs))
				notifier.Notify(subscription.ID, result)
			case <-subscription.Err():
				api.simulator.logger.Info("Subscription error, stopping goroutine", "subscriptionID", subscription.ID)
				return
			}
		}
	}()

	return subscription, nil
}
