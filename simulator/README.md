# Simple Simulator for Go Ethereum

A simple standalone simulator for go-ethereum that provides basic start/stop/status functionality and subscribes to new transactions from the transaction pool.

## Overview

This simulator is a minimal example that demonstrates how to create a simulator for go-ethereum. It provides:

- Start/Stop functionality
- Status checking
- RPC API integration
- Background task execution
- Transaction pool subscription
- **Pending state management** - Maintains a persistent state that evolves with transactions
- **Block event subscription** - Updates pending state on new block imports
- **Transaction simulation** - Executes transactions against the pending state
- **State persistence** - Maintains state changes between transaction executions
- Event emission for simulation results

## RPC API

The simulator exposes the following RPC methods under the `simulator` namespace:

### `simulator_start()`
Starts the simulator.

**Returns:**
- `null` on success
- Error object on failure

### `simulator_stop()`
Stops the simulator.

**Returns:**
- `null` on success
- Error object on failure

### `simulator_status()`
Gets the current status of the simulator.

**Returns:**
```json
{
    "running": true,
    "timestamp": "2024-01-01T12:00:00Z",
    "pendingStateInitialized": true,
    "pendingBlockNumber": 12345,
    "pendingBlockHash": "0x..."
}
```

### `simulator_subscribeSimulationResults()`
Subscribes to simulation results. Returns a subscription that emits simulation results for each transaction processed.

**Returns:**
```json
{
    "transaction": {
        "hash": "0x...",
        "from": "0x...",
        "to": "0x...",
        "value": "0x...",
        "data": "0x..."
    },
    "logs": [
        {
            "address": "0x...",
            "topics": ["0x..."],
            "data": "0x...",
            "blockNumber": 12345,
            "transactionHash": "0x...",
            "transactionIndex": 0,
            "blockHash": "0x...",
            "logIndex": 0,
            "removed": false
        }
    ],
    "success": true,
    "error": null,
    "blockNumber": 12346
}
```

**Note:** The `blockNumber` field represents the pending block number (latest + 1) where the transaction was simulated.

## Example Usage

### Using the JavaScript Console:

```javascript
// Get simulator status (will show running: false initially)
simulator.status()

// Start the simulator manually
simulator.start()

// Stop the simulator manually
simulator.stop()

// Check if simulator is running
simulator.status().running
```

### Using curl to interact with the simulator API:

```bash
# Get simulator status (will show running: false initially)
curl -X POST -H "Content-Type: application/json" --data '{"jsonrpc":"2.0","method":"simulator_status","params":[],"id":1}' http://localhost:8545

# Start the simulator manually
curl -X POST -H "Content-Type: application/json" --data '{"jsonrpc":"2.0","method":"simulator_start","params":[],"id":1}' http://localhost:8545

# Stop the simulator manually
curl -X POST -H "Content-Type: application/json" --data '{"jsonrpc":"2.0","method":"simulator_stop","params":[],"id":1}' http://localhost:8545

# Subscribe to simulation results (WebSocket)
wscat -c ws://localhost:8546 -x '{"jsonrpc":"2.0","method":"simulator_subscribeSimulationResults","params":[],"id":1}'
```

## How It Works

### Pending State Management
The simulator maintains a **pending state** that evolves as transactions are processed:

1. **Initialization**: When started, the simulator initializes the pending state from the latest block with block number incremented by 1
2. **Transaction Processing**: Each transaction is executed against the current pending state
3. **State Evolution**: After each successful transaction, the pending state is updated with the changes
4. **Block Updates**: When a new block is imported, the pending state is refreshed from the new block with block number incremented by 1
5. **Global Pending State**: The simulator's pending state becomes the authoritative pending state for the entire Ethereum node when `rpc.PendingBlockNumber` is requested

### Transaction Flow
```
New Transaction → Check Chain ID → Check Transaction Type → Execute on Pending State → Update Pending State → Emit Result
```

**Note:** The simulator only processes transactions that:
- Match the current network's chain ID
- Are not vanilla transactions (plain ETH transfers with 21000 gas)
- Transactions from other networks or vanilla transfers are skipped with clear error messages.

### Block Event Handling
```
New Block Imported → Update Pending State → Continue Processing Transactions
```

### Global Pending State Integration
When the simulator is running, it replaces the miner's pending state for all `rpc.PendingBlockNumber` requests:

- **eth_getBlockByNumber("pending")**: Returns simulator's pending header
- **eth_getHeaderByNumber("pending")**: Returns simulator's pending header  
- **eth_call with "pending"**: Uses simulator's pending state
- **Fallback**: If simulator is not running, falls back to miner's pending state

## Extending the Simulator

To add your own functionality, you can modify the `run()` method in `simulator.go`. Currently, it logs a heartbeat every 30 seconds and simulates transactions from the transaction pool, but you can add any logic you need:

```go
func (s *Simulator) run() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			// Add your custom logic here
			s.logger.Info("Simulator heartbeat", "timestamp", time.Now())
		case ev := <-s.txCh:
			// Process new transactions
			s.logger.Info("New transaction received", "count", len(ev.Txs))
			for _, tx := range ev.Txs {
				// Simulate the transaction against pending state
				result := s.simulateTransaction(tx)
				
				// Add your custom processing logic here
				if result.Success {
					s.logger.Info("Transaction simulation successful", 
						"hash", tx.Hash().Hex(), 
						"logs", len(result.Logs))
				} else {
					s.logger.Warn("Transaction simulation failed", 
						"hash", tx.Hash().Hex(), 
						"error", result.Error)
				}
				
				// Emit the simulation result
				s.resultFeed.Send(result)
			}
		case ev := <-s.blockCh:
			// Handle new block events
			s.logger.Info("New block imported", 
				"blockNumber", ev.Header.Number.Uint64(), 
				"blockHash", ev.Header.Hash().Hex())
			
			// Update pending state with the new block
			if err := s.updatePendingStateFromHeader(ev.Header); err != nil {
				s.logger.Error("Failed to update pending state", "error", err)
			}
		}
	}
}
```

## Testing

Run the tests with:

```bash
go test ./simulator/...
```

## Integration

The simulator is integrated into the Ethereum node but requires manual control:

- **Initialized** when the node starts (but not running)
- **Started manually** via `simulator_start()` RPC method
- **Stopped manually** via `simulator_stop()` RPC method
- **Auto-stopped** when the node shuts down (if running)
- **Exposes RPC APIs** for control and subscription
