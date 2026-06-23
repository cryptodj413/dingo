// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package models

// AccountDelegationHistoryRow holds delegation history
// query results for a stake account.
type AccountDelegationHistoryRow struct {
	AddedSlot   uint64
	BlockIndex  uint32
	CertIndex   uint32
	TxHash      []byte
	PoolKeyHash []byte
}

// AccountRegistrationHistoryRow holds registration
// history query results for a stake account.
type AccountRegistrationHistoryRow struct {
	AddedSlot  uint64
	BlockIndex uint32
	CertIndex  uint32
	TxHash     []byte
	Action     string
}

// AccountDrepAtSlot holds the DRep delegation state for a stake account
// as of a specific point in chain history. Returned by
// GetAccountsDrepDelegationAtSlot for use in historical auto-vote resolution.
type AccountDrepAtSlot struct {
	// DrepType is the DRep delegation type at or before the queried slot.
	// Only meaningful when HasData is true.
	DrepType uint64
	// Active is true when the account was registered and not deregistered
	// at the queried slot.
	Active bool
	// HasData is true when at least one registration certificate at or
	// before the queried slot was found for this account. When false the
	// caller must treat the result as "unknown" — not as "no DRep delegation".
	HasData bool
}
