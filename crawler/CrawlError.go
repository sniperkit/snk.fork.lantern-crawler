/*
Sniperkit-Bot
- Status: analyzed
*/

package crawler

type CrawlError struct {
	message string
}

func (e *CrawlError) Error() string {
	return e.message
}
