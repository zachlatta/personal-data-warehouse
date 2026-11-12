package ingestclient

import (
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// ObservationPoster posts one SourceObservation; Client.PostSourceObservation
// satisfies it, and an uploader Runner holds one as its Observe field (nil
// disables observing, as in most tests).
type ObservationPoster func(SourceObservation) (map[string]any, error)

// Observe posts what a device's own store says is its newest item (contract
// C11) and never fails the caller: the observation is evidence for a
// detector, so a failed post is a warning, and the uploader's own data must
// still go up. A missed observation reads `unknown` once it ages, which is
// the honest verdict. A zero newestAt means the store holds nothing.
func Observe(post ObservationPoster, logger common.Logger, pipeline, scope string, newestAt time.Time, newestID, detail string, observedAt time.Time) {
	if post == nil {
		return
	}
	obs := SourceObservation{
		Pipeline:   pipeline,
		Scope:      scope,
		Detail:     detail,
		ObservedAt: common.ISOFormat(observedAt),
	}
	if !newestAt.IsZero() && newestAt.Unix() > 0 {
		obs.NewestAt = common.ISOFormat(newestAt)
		obs.NewestID = newestID
	}
	if _, err := post(obs); err != nil && logger != nil {
		logger.Warningf("Could not post the %s source observation: %v", pipeline, err)
	}
}
