package storage

import "testing"

// appTableTags lists EVERY application-keyspace table tag. AppKeyspaceBounds() must
// cover exactly these (and must exclude the raft log / stable-store tags). When you
// add a new table tag, register it here too: the assertions below force the snapshot
// bound to be extended to cover it. A tag left outside the bound is silently dropped
// from snapshot/restore, which diverges a restoring or catching-up follower
// permanently — the single most dangerous, easiest-to-miss keyspace mistake.
var appTableTags = []byte{
	tagMeta, tagMessage, tagGroupMeta, tagLease, tagTimeIndex, tagDLQ,
	tagPolicy, tagCron, tagSingleton, tagToken, tagLaneConfig, tagDedup,
	tagWFRun, tagWFHistory, tagWFActivityDn, tagAuthPrincipal,
}

func TestAppKeyspaceBoundsCoverAllAppTags(t *testing.T) {
	lo, hi := AppKeyspaceBounds()
	if len(lo) != 1 || len(hi) != 1 {
		t.Fatalf("AppKeyspaceBounds returned multi-byte bounds %x..%x; expected single-byte tag bounds", lo, hi)
	}
	loB, hiB := lo[0], hi[0]

	var maxTag byte
	for _, tag := range appTableTags {
		if tag < loB || tag >= hiB {
			t.Errorf("app table tag 0x%02X is outside AppKeyspaceBounds [0x%02X, 0x%02X) — snapshot/restore would silently drop it; extend the bound", tag, loB, hiB)
		}
		if tag > maxTag {
			maxTag = tag
		}
	}
	// The bound must be tight: exactly one past the highest app tag. If this fails
	// after you added a tag, bump AppKeyspaceBounds()'s upper bound; if it fails
	// without a new tag, you forgot to register the tag in appTableTags above.
	if hiB != maxTag+1 {
		t.Errorf("AppKeyspaceBounds upper = 0x%02X, want 0x%02X (highest app tag 0x%02X + 1)", hiB, maxTag+1, maxTag)
	}
	if loB != tagMeta {
		t.Errorf("AppKeyspaceBounds lower = 0x%02X, want 0x%02X (tagMeta)", loB, tagMeta)
	}

	// The raft log / stable store must stay OUTSIDE the app bound: an FSM snapshot
	// must never clobber a node's own raft log on restore.
	for _, tag := range []byte{tagRaftLog, tagRaftKV} {
		if tag >= loB && tag < hiB {
			t.Errorf("raft tag 0x%02X is inside the app bound [0x%02X, 0x%02X) — snapshot would clobber the raft log", tag, loB, hiB)
		}
	}
}
