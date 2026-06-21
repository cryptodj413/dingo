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

package database

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	sqliteplugin "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/stretchr/testify/require"
)

// TestResolvePoolRewardAccountAutoVotesMissingPoolStaysUnresolved asserts
// that a snapshot whose pool key has no matching row in the database is
// left with RewardAccountAutoVoteResolved=false and
// RewardAccountAutoVote=None after resolution. This guards against the
// Mithril snapshot-pool fallback ordering bug: if pool rows are absent
// when the resolver runs (because they have not been imported yet), the
// row must not be persisted as authoritatively resolved.
func TestResolvePoolRewardAccountAutoVotesMissingPoolStaysUnresolved(
	t *testing.T,
) {
	db, err := New(&Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	pkh := make([]byte, 28)
	pkh[0] = 0xAB

	snapshot := &models.PoolStakeSnapshot{
		Epoch:        10,
		SnapshotType: "mark",
		PoolKeyHash:  pkh,
		TotalStake:   500,
	}

	require.NoError(t, db.ResolvePoolRewardAccountAutoVotes(
		[]*models.PoolStakeSnapshot{snapshot}, nil,
	))

	require.False(t, snapshot.RewardAccountAutoVoteResolved,
		"pool row absent: row must not be marked as authoritatively resolved")
	require.Equal(
		t,
		models.PoolRewardAccountAutoVoteNone,
		snapshot.RewardAccountAutoVote,
		"pool row absent: auto-vote value must be None",
	)
}

// TestResolvePoolRewardAccountAutoVotesMixedPresence verifies that when
// some pool keys exist in the database and others do not, only the found
// pools' snapshots are marked resolved. This covers the partial-import
// scenario where a fallback pool import may not include every pool that
// appears in the snapshot bundle.
func TestResolvePoolRewardAccountAutoVotesMixedPresence(t *testing.T) {
	db, err := New(&Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	store, ok := db.Metadata().(*sqliteplugin.MetadataStoreSqlite)
	require.True(t, ok, "test requires sqlite metadata backend")

	presentPKH := make([]byte, 28)
	presentPKH[0] = 0x01
	rewardAccount := make([]byte, 28)
	rewardAccount[0] = 0x02
	absentPKH := make([]byte, 28)
	absentPKH[0] = 0x03

	require.NoError(t, store.DB().Create(&models.Pool{
		PoolKeyHash:   presentPKH,
		RewardAccount: rewardAccount,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: rewardAccount,
		DrepType:   models.DrepTypeAlwaysAbstain,
		AddedSlot:  1,
		Active:     true,
	}).Error)

	presentSnap := &models.PoolStakeSnapshot{
		Epoch:        20,
		SnapshotType: "mark",
		PoolKeyHash:  presentPKH,
		TotalStake:   100,
	}
	absentSnap := &models.PoolStakeSnapshot{
		Epoch:        20,
		SnapshotType: "mark",
		PoolKeyHash:  absentPKH,
		TotalStake:   200,
	}

	require.NoError(t, db.ResolvePoolRewardAccountAutoVotes(
		[]*models.PoolStakeSnapshot{presentSnap, absentSnap}, nil,
	))

	require.True(t, presentSnap.RewardAccountAutoVoteResolved,
		"pool row found: snapshot must be marked resolved")
	require.Equal(t, models.PoolRewardAccountAutoVoteAbstain,
		presentSnap.RewardAccountAutoVote,
		"pool found with AlwaysAbstain delegation: auto-vote must be Abstain")

	require.False(t, absentSnap.RewardAccountAutoVoteResolved,
		"pool row absent: snapshot must not be marked resolved")
	require.Equal(t, models.PoolRewardAccountAutoVoteNone,
		absentSnap.RewardAccountAutoVote,
		"pool row absent: auto-vote must be None")
}

// TestResolvePoolRewardAccountAutoVotesFoundPoolNoRewardAccount checks
// that a pool row with an empty reward account is still marked resolved:
// the pool is known, so the absence of a predefined-DRep delegation is a
// real "none" answer, not "unknown".
func TestResolvePoolRewardAccountAutoVotesFoundPoolNoRewardAccount(
	t *testing.T,
) {
	db, err := New(&Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	store, ok := db.Metadata().(*sqliteplugin.MetadataStoreSqlite)
	require.True(t, ok, "test requires sqlite metadata backend")

	pkh := make([]byte, 28)
	pkh[0] = 0x55

	require.NoError(t, store.DB().Create(&models.Pool{
		PoolKeyHash:   pkh,
		RewardAccount: nil,
	}).Error)

	snap := &models.PoolStakeSnapshot{
		Epoch:        30,
		SnapshotType: "mark",
		PoolKeyHash:  pkh,
		TotalStake:   300,
	}

	require.NoError(t, db.ResolvePoolRewardAccountAutoVotes(
		[]*models.PoolStakeSnapshot{snap}, nil,
	))

	require.True(t, snap.RewardAccountAutoVoteResolved,
		"pool row found (even with no reward account): snapshot must be resolved")
	require.Equal(t, models.PoolRewardAccountAutoVoteNone, snap.RewardAccountAutoVote)
}
