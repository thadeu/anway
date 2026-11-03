package httpapi

import (
	"net/http"
	"slices"

	"github.com/thadeu/anway/internal/manager"
	"github.com/thadeu/anway/internal/storage"
)

// topics lists each topic with its subscriptions, and for each subscription
// the connected workers that fetch from its queue. A subscription with no
// worker has copies that wait until a worker fetches from its queue.
func (a *API) topics(w http.ResponseWriter, _ *http.Request) {
	subscriptions, err := a.manager.Subscriptions("")
	if err != nil {
		a.fail(w, err)

		return
	}

	workers := a.manager.Workers()
	views := []map[string]any{}

	for _, subscription := range subscriptions {
		last := len(views) - 1

		if last < 0 || views[last]["name"] != subscription.Topic {
			views = append(views, map[string]any{"name": subscription.Topic, "subscriptions": []map[string]any{}})
			last++
		}

		views[last]["subscriptions"] = append(views[last]["subscriptions"].([]map[string]any), subscriptionView(subscription, workers))
	}

	writeJSON(w, http.StatusOK, map[string]any{"topics": views})
}

func subscriptionView(subscription storage.Subscription, workers []manager.WorkerInfo) map[string]any {
	listeners := []map[string]any{}

	for _, worker := range workers {
		if worker.Connections == 0 || !slices.Contains(worker.Queues, subscription.Queue) {
			continue
		}

		listeners = append(listeners, map[string]any{
			"wid":      worker.WID,
			"hostname": worker.Hostname,
			"pid":      worker.PID,
			"tag":      worker.Tag,
			"labels":   nonNil(worker.Labels),
		})
	}

	return map[string]any{
		"queue":       subscription.Queue,
		"jobtype":     subscription.JobType,
		"retry":       subscription.Retry,
		"reserve_for": subscription.ReserveFor,
		"created_at":  subscription.CreatedAt,
		"workers":     listeners,
	}
}
