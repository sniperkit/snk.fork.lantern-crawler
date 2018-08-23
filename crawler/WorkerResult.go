/*
Sniperkit-Bot
- Status: analyzed
*/

package crawler

import (
	"net/url"
)

type WorkerResult struct {
	Links []*url.URL
}

func NewWorkerResult() *WorkerResult {
	return &WorkerResult{
		Links: make([]*url.URL, 0, 5),
	}
}

func (r *WorkerResult) Append(url *url.URL) {
	r.Links = append(r.Links, url)
}

// The pop channel is a stacked channel used by workers to pop the next URL(s)
// to process.
type WorkerResultChannel chan *WorkerResult

// Constructor to create and initialize a popChannel
func NewWorkerResultChannel() WorkerResultChannel {
	// The pop channel is stacked, so only a buffer of 1 is required
	// see http://gowithconfidence.tumblr.com/post/31426832143/stacked-channels
	return make(chan *WorkerResult, 1)
}

// The stack function ensures the specified URLs are added to the pop channel
// with minimal blocking (since the channel is stacked, it is virtually equivalent
// to an infinitely buffered channel).
// Returns the current length of the stack
func (wc WorkerResultChannel) stack(result *WorkerResult) int {
	for {
		select {
		case wc <- result:
			return len(result.Links)
		case old := <-wc:
			// Content of the channel got emptied and is now in old, so append whatever
			// is in arr, to it, so that it can either be inserted in the channel,
			// or appended to some other content that got through in the meantime.
			result.Links = append(old.Links, result.Links...)
		}
	}
}
