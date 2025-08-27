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

package simulator

import (
	"testing"

	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/log"
)

func TestSimulatorBasic(t *testing.T) {
	// Test basic functionality without backend dependency
	simulator := &Simulator{
		logger: log.New("module", "simulator"),
		stopCh: make(chan struct{}),
		txCh:   make(chan core.NewTxsEvent, 100),
		blockCh: make(chan core.ChainEvent, 10),
	}
	
	// Test initial state
	if simulator.IsRunning() {
		t.Fatalf("Simulator should not be running initially")
	}
	
	// Test that simulator can be created and has correct initial state
	if simulator.backend != nil {
		t.Fatalf("Backend should be nil in test")
	}
	
	// Test that pending state is not initialized initially
	if simulator.pendingState != nil || simulator.pendingHeader != nil {
		t.Fatalf("Pending state should be nil initially")
	}
}
