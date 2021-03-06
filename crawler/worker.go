/*
Sniperkit-Bot
- Status: analyzed
*/

package crawler

import (
	"bufio"
	"bytes"
	"fmt"

	//"github.com/PuerkitoBio/purell"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const maxRecrawlDepth = 3
const maxCrawlDepth = 40
const maxFileSize = 2000000

var onionRegexp = regexp.MustCompile("[^a-zA-Z2-7]+")

type Subdomain struct {
	Url          *url.URL
	Index        int
	AlreadyFound map[string]*CrawlItem
}

type Hostworker struct {
	Host   string // Domain without subdomains!
	Scheme string // Migrate automatically external domains?

	// Lijst met items die opnieuw moeten worden gecrawld met depth >= maxRecrawlDepth
	// Een item mag hier maximum 1 maand in verblijven (= vernieuwings interval)
	LowPriorityQueue *CrawlQueue

	// De queue bevat een lijst met items die nog nooit gecrawled werden met depth >= maxRecrawlDepth en
	// één voor één zullen worden gedownload. Deze hebben voorrang op recrawlen van pagina's met depth >= maxRecrawlDepth
	Queue *CrawlQueue

	// Lijst met items die een diepte hebben lager dan maxRecrawlDepth
	// Een item zou hier maximum 1 dag in mogen verblijven
	PriorityQueue *CrawlQueue

	// Lijst met items die te veel na elkaar mislukt zijn
	// Ze staan chonologisch gerangschikt. Degene die het langst geleden mislukt zijn,
	// staan vooraan in de wachtrij. Regelmatig controleren we of het eerste item ouder is dan 12 uur.
	// als dat zo is, halen we deze uit de lijst en verwijderen we deze uit AlreadyVisited om het opnieuw een kans te geven
	// om gecrawled te worden.
	FailedQueue *LeveledQueue

	// Hierin staan alle items die aanwezig zijn in Queue of PriorityQueue
	// OF items die niet aanwezig zijn in die queue's, maar die nog niet gedownload mogen worden
	// omdat item.NeedRecrawl false geeft. We
	// gaan deze af en toe uitkuisen en items die NeedsRecrawl true geven verwijderen
	// zo geraken we bewust pagina's kwijt waar nergens nog naar wordt verwezen
	Subdomains map[string]*Subdomain

	// Lijst met alle url's met diepte = 0. Deze staan cronologisch gerangschikt
	// van laatste gedownload naar meest recent gedownload
	IntroductionPoints *CrawlQueue

	Running         bool // Of goroutine loopt
	Sleeping        bool // Of deze worker in de sleeping queue aanwezig is
	InRecrawlList   bool
	RecrawlOnFinish bool // Enkel aanpassen of opvragen buiten de goroutine v/d worker

	Client   *http.Client
	stop     chan struct{}
	NewItems popChannel
	crawler  *Crawler

	// Aantal requests die nog voltooid moeten worden
	// voor hij overweegt om naar slaapstand te gaan
	// als er andere domeinen 'wachten'
	sleepAfter int

	LatestCycle int

	InMemory              bool
	cachedWantsToGetUp    bool
	cachedLastDownload    *time.Time
	cachedRecrawlOnMemory bool

	// Detecteren van tijdelijk onbeschikbare domeinen
	// na 10 achtereenvolgende fails zonder successvolle
	// verhogen we Failstreak. Daarna wachten we een bepaald aantal
	// dagen, weken, maanden voor we opnieuw proberen afhankelijk van de grootte van FailStreak
	// Opnieuw proberen doen we enkel als we weer een URL vinden naar dit domein
	FailCount      int /// Aantal mislukte downloads na elkaar
	FailStreak     int /// Aantal keer dat FailCount > 10
	LastFailStreak *time.Time
}

func (w *Hostworker) String() string {
	return w.Host
}

/**
 * Sla enkel de RecrawlQueue op. De AlreadyVisited maakt niet veel uit aangezien we deze uiteindelijk toch gaan opnieuw crawlen
 * als we de recrawl queue opnieuw crawlen.
 */
func (w *Hostworker) SaveToFile() bool {
	os.Mkdir("/etc/lantern/hosts", 0777)
	file, err := os.Create("/etc/lantern/hosts/host_" + w.Host + ".txt")
	if err != nil {
		w.crawler.cfg.LogError(err)
		return false
	}
	defer func() {
		file.Close()
	}()

	writer := bufio.NewWriter(file)
	w.SaveToWriter(writer)
	writer.Flush()
	return true
}

func NewHostWorkerFromFile(file *os.File, crawler *Crawler) *Hostworker {
	reader := bufio.NewReader(file)
	w := NewHostworker("", crawler)
	w.ReadFromReader(reader)
	w.HardReset()
	return w
}

func (w *Hostworker) MoveToDisk() {
	w.cachedWantsToGetUp = w.wantsToGetUp()
	if w.IntroductionPoints.IsEmpty() {
		w.cachedLastDownload = nil
	} else {
		w.cachedLastDownload = w.IntroductionPoints.First.LastDownload
	}

	if !w.SaveToFile() {
		return
	}
	w.InMemory = false
	w.IntroductionPoints = nil
	w.Subdomains = nil
	w.PriorityQueue = nil
	w.LowPriorityQueue = nil
	w.Queue = nil
	w.FailedQueue = nil
}

/// Move out of memory without save to file
func (w *Hostworker) HardReset() {
	w.cachedWantsToGetUp = w.WantsToGetUp()
	if w.IntroductionPoints.IsEmpty() {
		w.cachedLastDownload = nil
	} else {
		w.cachedLastDownload = w.IntroductionPoints.First.LastDownload
	}

	w.InMemory = false
	w.IntroductionPoints = nil
	w.Subdomains = nil
	w.PriorityQueue = nil
	w.LowPriorityQueue = nil
	w.Queue = nil
	w.FailedQueue = nil
}

func (w *Hostworker) MoveToMemory() {
	file, err := os.Open("/etc/lantern/hosts/host_" + w.Host + ".txt")
	if err != nil {
		w.crawler.cfg.LogError(err)
		panic("Coudn't move to memory: file not found")
	}
	defer file.Close()

	w.InMemory = true
	w.IntroductionPoints = NewCrawlQueue("Introduction points")
	w.Subdomains = make(map[string]*Subdomain)
	w.PriorityQueue = NewCrawlQueue("Priority Queue")
	w.LowPriorityQueue = NewCrawlQueue("Low Priority Queue")
	w.Queue = NewCrawlQueue("Queue")
	w.FailedQueue = NewLeveledQueue()
	if !w.ReadFromReader(bufio.NewReader(file)) {
		panic("Coudn't move to memory: file not readable")
	}

	if w.cachedRecrawlOnMemory {
		w.cachedRecrawlOnMemory = false
		w.Recrawl()
	}
}

func (w *Hostworker) GetRecrawlDuration() time.Duration {
	if !w.InMemory {
		if w.cachedLastDownload == nil {
			w.crawler.cfg.Log("error", "GetRecrawlDuration on worker with empty IntroductionPoints (disk)!")
			return time.Minute * 5
		}
		return time.Hour*12 - time.Since(*w.cachedLastDownload)
	}

	if w.IntroductionPoints.IsEmpty() {
		w.crawler.cfg.Log("error", "GetRecrawlDuration on worker with empty IntroductionPoints!")
		return time.Minute * 5
	}
	return time.Hour*12 - time.Since(*w.IntroductionPoints.First.LastDownload)
}

func NewHostworker(host string, crawler *Crawler) *Hostworker {
	w := &Hostworker{
		Host:               host,
		Scheme:             "http",
		Queue:              NewCrawlQueue("Queue"),
		PriorityQueue:      NewCrawlQueue("Priority Queue"),
		LowPriorityQueue:   NewCrawlQueue("Low Priority Queue"),
		IntroductionPoints: NewCrawlQueue("Introduction points"),
		FailedQueue:        NewLeveledQueue(),

		Subdomains: make(map[string]*Subdomain),
		NewItems:   newPopChannel(),
		stop:       crawler.Stop,
		crawler:    crawler,
		InMemory:   true,
	}

	return w
}

func (w *Hostworker) EmptyPendingItems() {
	if !w.InMemory {
		return
	}
	select {
	case q := <-w.NewItems:
		w.AddQueue(q)
	default:
		break
	}
}

func (w *Hostworker) NeedsWriteToDisk() bool {
	if w.InMemory {
		// Nieuwe host die nog nooit aan de beurt is gekomen
		return true
	} else {
		select {
		case q := <-w.NewItems:
			w.MoveToMemory()
			w.AddQueue(q)
			return true
		default:
			break
		}
	}
	return false
}
func (w *Hostworker) IsInFailTimeout() bool {
	if w.LastFailStreak == nil {
		return false
	}

	a := time.Since(*w.LastFailStreak) < time.Hour*time.Duration(w.FailStreak*w.FailStreak)/2
	if a == false {
		w.LastFailStreak = nil
	}
	return a
}

func (w *Hostworker) WantsToGetUp() bool {
	if w.IsInFailTimeout() {
		return false
	}

	if !w.InMemory {
		return w.cachedWantsToGetUp
	}

	return w.wantsToGetUp()
}

func (w *Hostworker) wantsToGetUp() bool {
	result := !w.PriorityQueue.IsEmpty() || !w.Queue.IsEmpty() || !w.LowPriorityQueue.IsEmpty()
	if result {
		return true
	}

	// Misschien hebben we een item in de failed queue die er al uit mag komen?
	failedItem := w.FailedQueue.First()
	if failedItem != nil && failedItem.FailCount < 4 {
		// Enkel requests starten met kleine kans op falen
		// de rest alleen starten als restjes
		return true
	}
	return false
}

func (w *Hostworker) AddQueue(q []*url.URL) {
	// Eerst nog overlopen op already visited, we kunnen dus niet rechtstreeks merge gebruiken
	for _, item := range q {
		w.NewReference(item, nil, false)
	}
}

/// Start een hercrawl cyclus. Voer dit enkel uit als de worker niet
/// 'aan' staat.
func (w *Hostworker) Recrawl() {
	if !w.InMemory {
		w.cachedWantsToGetUp = true
		w.cachedRecrawlOnMemory = true
		return
	}

	w.LatestCycle++

	if w.crawler.cfg.LogRecrawlingEnabled {
		w.crawler.cfg.LogInfo("Recrawl initiated for " + w.String())
	}

	item := w.IntroductionPoints.Pop()
	for item != nil {
		item.Cycle = w.LatestCycle
		w.PriorityQueue.Push(item)

		item = w.IntroductionPoints.Pop()
	}
}

func (w *Hostworker) Run(client *http.Client) {
	defer func() {
		if e := recover(); e != nil {
			//log and so other stuff
			w.crawler.cfg.Log("Panic", identifyPanic())
		}

		if w.InMemory {
			w.EmptyPendingItems()
			w.MoveToDisk()
		}

		// Aangeven dat deze goroutine afgelopen is
		w.crawler.waitGroup.Done()

		// Onze crawler terug wakker maken om eventueel een nieuwe request op te starten
		w.crawler.WorkerEnded.stack(w)

	}()

	if w.crawler.cfg.LogGoroutinesEnabled {
		w.crawler.cfg.LogInfo("Goroutine for host " + w.String() + " started")
	}

	w.Client = client

	w.sleepAfter = w.crawler.cfg.SleepAfter + rand.Intn(w.crawler.cfg.SleepAfterRandom)

	if !w.InMemory {
		w.MoveToMemory()
		if !w.InMemory {
			return
		}
		w.EmptyPendingItems()
	}

	for {
		select {
		case <-w.stop:
			return
		case q := <-w.NewItems:
			w.AddQueue(q)

		default:
			item := w.GetNextRequest()

			if item == nil {
				// queue is leeg
				return
			}

			w.RequestStarted(item)
			w.Request(item)

			if w.sleepAfter <= 0 {
				// Meteen stoppen
				return
			}

			time.Sleep(time.Millisecond * time.Duration(w.crawler.cfg.SleepTime+rand.Intn(w.crawler.cfg.SleepTimeRandom)))

		}
	}
}

func (w *Hostworker) Request(item *CrawlItem) {

	// todo: . / .. splitten verwijderen in ResolveReference
	// en misschien meteen string van maken?
	reqUrl := item.Subdomain.Url.ResolveReference(item.URL)

	if w.crawler.cfg.LogRequests {
		w.crawler.cfg.LogInfo("New request " + reqUrl.String())
	}

	if request, err := http.NewRequest("GET", reqUrl.String(), nil); err == nil {
		request.Header.Add("User-Agent", "Mozilla/5.0 (Windows NT 6.1; rv:45.0) Gecko/20100101 Firefox/45.0")
		request.Header.Add("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
		request.Header.Add("Accept_Language", "en-US,en;q=0.5")
		request.Header.Add("Connection", "keep-alive")

		//request.Close = true // Connectie weggooien
		request = request.WithContext(w.crawler.context)

		if response, err := w.Client.Do(request); err == nil {
			defer response.Body.Close()

			if response.StatusCode < 200 || response.StatusCode >= 300 {
				if w.crawler.cfg.LogNetwork {
					w.crawler.cfg.Log("network", fmt.Sprintf("status %v %s", response.StatusCode, reqUrl))
				}

				// Special exceptions
				if response.StatusCode == 429 {
					w.crawler.cfg.Log("WARNING", "Too many requests for host "+w.String())
					w.NewFailStreak()
					w.RequestFailed(item)
					return
				}

				// ignore range: 400 - 406
				if response.StatusCode >= 400 && response.StatusCode <= 406 {
					if w.crawler.cfg.LogNetwork {
					}
					w.RequestIgnored(item)
					return
				}

				if response.StatusCode >= 400 && response.StatusCode < 500 {
					// Lange tijd wachten voor het nog 2x opnieuw te proberen
					if item.FailCount < maxFailCount-2 {
						item.FailCount = maxFailCount - 2
					}

					w.RequestFailed(item)
				} else {
					// Retry eventually
					w.RequestFailed(item)
				}
				return
			}

			startTime := time.Now()

			// Maximaal 2MB (pagina's in darkweb zijn gemiddeld erg groot vanwege de afbeeldingen)
			if response.ContentLength > maxFileSize {
				//w.crawler.cfg.LogInfo("Response: Content too long")
				// Too big
				// Eventueel op een ignore list zetten
				if w.crawler.cfg.LogNetwork {
					w.crawler.cfg.Log("network", "file too big (content length) "+reqUrl.String())
				}

				w.RequestIgnored(item)
				return
			}

			// Eerste 512 bytes lezen om zo de contentType te bepalen
			b, err := readFirstBytes(response.Body)
			if err != nil {
				// Er ging iets mis
				//w.crawler.cfg.LogError(err)
				w.RequestFailed(item)
				return
			}

			// Content type inlezen, als die niet goed zit stoppen...
			contentType := http.DetectContentType(b)
			//w.crawler.cfg.LogInfo("Detected Content-Type: " + contentType)

			if contentType != "text/html; charset=utf-8" {
				//w.crawler.cfg.LogInfo("Not a HTML file")
				// Op ignore list zetten
				if w.crawler.cfg.LogNetwork {
					w.crawler.cfg.Log("network", "not a html file "+reqUrl.String())
				}

				w.RequestIgnored(item)
				return
			}

			firstReader := bytes.NewReader(b)

			// De twee readers terug samenvoegen
			reader := NewCountingReader(io.MultiReader(firstReader, response.Body), maxFileSize)
			if w.ProcessResponse(item, response, reader) {
				duration := time.Since(startTime)
				w.crawler.speedLogger.Log(duration, reader.Size)
			}

		} else {

			if response != nil && response.Body != nil {
				response.Body.Close()
			}

			str := err.Error()
			if w.crawler.cfg.LogNetwork {
				w.crawler.cfg.Log("network", str)
			}

			if strings.Contains(str, "SOCKS5") {
				// Er is iets mis met de proxy,
				// zal zich normaal uatomatisch herstellen, maar
				// we stoppen even met deze crawler
				w.sleepAfter = -1

			} else if strings.Contains(str, "Client.Timeout") {
				if item.FailCount == 0 {
					w.crawler.speedLogger.LogTimeout()
				}
			} else if strings.Contains(str, "timeout awaiting response headers") {
				if item.FailCount == 0 {
					w.crawler.speedLogger.LogTimeout()
				}
			} else if strings.Contains(str, "stopped after 10 redirects") {
				w.RequestIgnored(item)
				return
			} else if strings.Contains(str, "server gave HTTP response to HTTPS client") {
				w.Scheme = "http"
				item.Subdomain.Url.Scheme = "http"
			} else if strings.Contains(str, "context canceled") {
				// Negeer failcount bij handmatige cancel
				item.FailCount--
				w.RequestFailed(item)
				return
			}

			if item.FailCount == 0 {
				w.FailCount++
				if w.FailCount > 40 {
					w.NewFailStreak()
				}
			}

			w.RequestFailed(item)
		}
	} else {
		w.crawler.cfg.LogError(err)
		w.RequestFailed(item)
	}

}

func (w *Hostworker) ProcessResponse(item *CrawlItem, response *http.Response, reader io.Reader) bool {
	// Doorgeven aan parser
	result, err := Parse(reader, w.crawler.Queries, item.Depth < maxCrawlDepth)

	if err != nil {
		if err.Error() == "Reader reached maximum bytes!" {
			if w.crawler.cfg.LogNetwork {
				w.crawler.cfg.Log("network", "file too big "+item.String())
			}
			w.RequestIgnored(item)
			return false
		}
		w.crawler.speedLogger.LogTimeout()
		w.RequestFailed(item)
		return false
	}

	if response.Request.URL.Scheme == "https" {
		w.Scheme = "https"
	} else if response.Request.URL.Scheme == "http" {
		w.Scheme = "http"
	}

	// tijdelijk absolute url toelaten!!!!!! -> makeRelative(item.URL) noodzakelijk achteraan
	item.URL = response.Request.URL

	// Save results
	if len(result.Results) > 0 {
		host := w.String()
		urlString := item.URL.String()

		for _, apiResult := range result.Results {
			apiResult.Host = &host
			apiResult.Url = &urlString
			if apiResult.Title == nil {
				apiResult.Title = &host
			}

			w.crawler.ApiController.SaveResult(apiResult)
		}
	}

	workerResult := NewWorkerResult()

	if result.Urls != nil {
		for _, u := range result.Urls {
			// Convert links to absolute url
			ResolveReferenceNoCopy(response.Request.URL, u)

			// Url moet absoluut zijn
			if !u.IsAbs() {
				panic("Resolve reference didn't make absolute")
				continue
			}

			if !strings.HasPrefix(u.Scheme, "http") || len(u.Host) == 0 {
				continue
			}

			// Host opspliten in subdomein en domein
			domains := strings.Split(u.Host, ".")
			if len(domains) < 2 {
				continue
			}

			if w.crawler.cfg.OnlyOnion {
				tld := domains[len(domains)-1]
				if tld != "onion" {
					continue
				}

				domain := domains[len(domains)-2]

				if len(domain) != 16 {
					// todo: ondersteuning voor tor subdomains toevoegen!
					// Ongeldig -> verwijder alle ongeldige characters (tor browser doet dit ook)
					domain = onionRegexp.ReplaceAllString(domain, "")
					if len(domain) != 16 {
						continue
					}
					// Terug samenvoegen
					domains[len(domains)-2] = domain
					u.Host = strings.Join(domains, ".")
				}
			} else {
				if len(domains[len(domains)-1]) < 2 {
					// tld te kort
					continue
				}

				if len(domains[len(domains)-2]) < 1 {
					// domain te kort
					continue
				}
			}

			if w.crawler.GetDomainForUrl(domains) == w.Host {
				// Interne URL's meteen verwerken
				w.NewReference(u, item, true)
			} else {
				workerResult.Append(u)
			}
		}
	}

	// Kritieke move operatie uitvoeren noodzakelijk?
	splitted := strings.Split(item.URL.Host, ".")
	if w.crawler.GetDomainForUrl(splitted) != w.Host {
		// Kopie maken van volledige absolute url en dan pas relatief maken
		cc := *item.URL
		makeRelative(item.URL)

		// Negeren vanaf nu voor deze worker
		w.RequestIgnored(item)

		// Is dit wel een onion, anders weg smijten
		if w.crawler.cfg.OnlyOnion {
			if len(splitted) < 2 {
				return false
			}

			tld := splitted[len(splitted)-1]
			if tld != "onion" {
				return false
			}
		}

		// Doorgeven aan crawler en aan juiste worker bezorgen voor verdere afhandeling?
		workerResult.Append(&cc)
		w.crawler.WorkerResult.stack(workerResult)

		return false
	}

	// Relatief maken
	makeRelative(item.URL)

	// Resultaat doorgeven aan Crawler
	if len(workerResult.Links) > 0 {
		w.crawler.WorkerResult.stack(workerResult)
	}

	w.RequestFinished(item)
	return true
}

func (w *Hostworker) RequestStarted(item *CrawlItem) {
	w.sleepAfter--

	//w.crawler.cfg.LogInfo(fmt.Sprintf("Request started. %v", item.URL.String()))
	now := time.Now()
	item.LastDownloadStarted = &now
}

func (w *Hostworker) RequestFinished(item *CrawlItem) {
	if w.crawler.cfg.LogRequests {
		w.crawler.cfg.LogInfo("Request finished" + item.URL.String())
	}

	w.FailStreak = 0
	w.LastFailStreak = nil
	w.FailCount = 0

	if item.Depth == 0 {
		// Introduction point toevoegen
		if w.IntroductionPoints.IsEmpty() {
			w.IntroductionPoints.Push(item)

			// Crawler verwittigen zodat we op de recrawl lijst komen
			w.crawler.WorkerIntroduction.stack(w)
		} else {
			if w.IntroductionPoints.Length < 10 {
				w.IntroductionPoints.Push(item)
			} else {
				// Check of de url een hoofd url is
				if item.URL.String() == "/" || item.URL.String() == "" {
					// todo: misschien tot bepaalde lengte of aantal '/' toestaan?
					w.IntroductionPoints.Push(item)
				}
			}
		}
	}

	if item.FailCount > 0 {
		item.FailCount = 0
	}

	now := time.Now()
	item.LastDownload = &now
	item.LastDownloadStarted = nil
}

func (w *Hostworker) RequestIgnored(item *CrawlItem) {
	if w.crawler.cfg.LogRequests {
		w.crawler.cfg.LogInfo("Request ignored" + item.URL.String())
	}

	item.Ignore = true
}

func (w *Hostworker) RequestFailed(item *CrawlItem) {
	if w.crawler.cfg.LogRequests {
		w.crawler.cfg.LogInfo("Request failed" + item.URL.String())
	}
	item.FailCount++

	if !item.IsUnavailable() {
		// We wagen nog een poging binnen een uurtje
		// Toevoegen aan failqueue
		w.FailedQueue.Push(item, item.FailCount)
	}
}

func (w *Hostworker) NewFailStreak() {
	w.FailCount = 0
	w.FailStreak++

	now := time.Now()
	w.LastFailStreak = &now

	// Meteen stoppen
	w.sleepAfter = -1
}

func (w *Hostworker) GetNextRequest() *CrawlItem {

	f := w.FailedQueue.First()

	if f != nil && f.FailCount < 2 {
		f = w.FailedQueue.Pop()
		return f
	}

	if !w.PriorityQueue.IsEmpty() {
		return w.PriorityQueue.Pop()
	}

	if !w.Queue.IsEmpty() {
		return w.Queue.Pop()
	}

	if !w.LowPriorityQueue.IsEmpty() {
		return w.LowPriorityQueue.Pop()
	}

	// Nu pas langdurige fails aan bod laten
	return w.FailedQueue.Pop()
}

func cleanURLPath(u *url.URL) string {
	str := []byte(u.String())
	if len(str) == 0 {
		return string(str)
	}

	// Remove trailing /
	if str[len(str)-1] == '/' {
		return string(str[:len(str)-1])
	}

	return string(str)
}

// Updates the absolute url to become relative
// Returns the url without path of the absolute url
func splitUrlRelative(absolute *url.URL) *url.URL {
	domain := *absolute
	domain.Path = ""
	domain.RawQuery = ""
	domain.ForceQuery = false

	absolute.Scheme = ""
	absolute.Host = ""
	absolute.User = nil
	absolute.ForceQuery = false
	// Query newslines en tabs verwijderen
	absolute.RawQuery = strings.Replace(strings.Replace(absolute.RawQuery, "\n", "%0A", -1), "\t", "%09", -1)

	return &domain
}

func makeRelative(absolute *url.URL) {
	absolute.Scheme = ""
	absolute.Host = ""
	absolute.User = nil
	absolute.ForceQuery = false
	// Query newslines en tabs verwijderen
	absolute.RawQuery = strings.Replace(strings.Replace(absolute.RawQuery, "\n", "%0A", -1), "\t", "%09", -1)

}

/**
 * Als internal = false mag sourceItem = nil
 */
func (w *Hostworker) NewReference(foundUrl *url.URL, sourceItem *CrawlItem, internal bool) (*CrawlItem, error) {
	// Create copy
	cc := *foundUrl
	foundUrl = &cc

	if w.IsInFailTimeout() {
		// failStreak detecteren en referenties gewoon
		// wegsmijten als we in timeout interval zitten
		return nil, nil
	}

	if !w.InMemory {
		count := w.NewItems.stack(foundUrl)
		if count > 50 {
			w.cachedWantsToGetUp = true
		}
		return nil, nil
	}

	if !foundUrl.IsAbs() {
		return nil, nil
	}

	subdomain, subdomainFound := w.Subdomains[foundUrl.Host]
	var item *CrawlItem
	var found bool

	if !subdomainFound {
		subdomainUrl := splitUrlRelative(foundUrl)
		subdomain = &Subdomain{Url: subdomainUrl, AlreadyFound: make(map[string]*CrawlItem)}
		w.Subdomains[subdomainUrl.Host] = subdomain
	} else {
		makeRelative(foundUrl)
	}

	// Vanaf nu mag foundUrl.Host niet meer gebruikt worden! Deze bestaat niet meer
	uri := cleanURLPath(foundUrl)

	if subdomainFound {
		item, found = subdomain.AlreadyFound[uri]
	}

	if !found {
		item = NewCrawlItem(foundUrl)
		item.Subdomain = subdomain

		if internal {
			item.Cycle = sourceItem.Cycle
		} else {
			// New introduction point
			item.Cycle = w.LatestCycle

			// Schema meteen juist zetten
			if !subdomainFound {
				subdomain.Url.Scheme = w.Scheme
			}
		}

		subdomain.AlreadyFound[uri] = item
	} else {
		if item.IsUnavailable() {
			// Deze url is onbereikbaar, ofwel geen HTML bestand
			// dat weten we omdat we deze al eerder hebben gecrawled
			return item, nil
		}

		if item.FailCount > 0 && !item.NeedsRetry() {
			return item, nil
		}
	}

	// Depth aanpassen
	if !internal {
		// Referentie vanaf een ander domein
		item.Depth = 0

	} else {
		if !found || item.Depth > sourceItem.Depth+1 {
			item.Depth = sourceItem.Depth + 1
		}
	}

	if internal && item.Cycle < sourceItem.Cycle {
		// Als een nieuwere cycle refereert naar deze pagina, dan kan
		// die de depth verhogen. Dit kan slechts één keer gebeuren,
		// aangezien hierna de cycle terug wordt gelijk gesteld
		// Daarna kan de depth enkel nog verlagen tot de volgende cycle
		// Op die manier houdt het systeem rekening met verloren / gewijzigde referenties

		item.Depth = sourceItem.Depth + 1
	}

	if item.Depth < maxRecrawlDepth && (item.Queue == w.Queue || item.Queue == w.LowPriorityQueue) {
		// Dit item staat nog in de gewone queue, maar heeft nu wel prioriteit
		// we verplaatsen het
		item.Remove()
		w.PriorityQueue.Push(item)

	} else if item.Queue == nil && (!found || (internal && item.Cycle < sourceItem.Cycle)) {
		// Recrawl enkel toelaten als we dit item nog niet gevonden hebben
		// of we hebben het wel al gevonden en het is een interne link afkomstig van een
		// hogere cycle (recrawl). Externe links die we al gecrawled hebben
		// negeren we, die staan in de introduction queue

		if item.Depth < maxRecrawlDepth {
			w.PriorityQueue.Push(item)
		} else {
			if !found {
				w.Queue.Push(item)
			} else {
				w.LowPriorityQueue.Push(item)
			}
		}
	} else if item.Queue != nil && item.Queue.Name == "failqueue" {
		// Uit huidige wachtrij verwijderen
		item.Remove()

		if item.Depth < maxRecrawlDepth {
			w.PriorityQueue.Push(item)
		} else {
			w.LowPriorityQueue.Push(item)
		}
	}

	// Cycle aanpassen
	if internal && item.Cycle < sourceItem.Cycle {
		item.Cycle = sourceItem.Cycle
	}

	return item, nil
}

//
//
// Saving functions
//
//

func (w *Hostworker) ReadFromReader(reader *bufio.Reader) bool {
	// Eerst de basis gegevens:
	line, _, _ := reader.ReadLine()
	if len(line) == 0 {
		return false
	}
	str := string(line)
	parts := strings.Split(str, "\t")

	if len(parts) == 6 {
		w.Host = parts[0]
		w.Scheme = parts[1]

		num, err := strconv.Atoi(parts[2])
		if err != nil {
			fmt.Println("Invalid failstreak")
			return false
		}
		w.FailStreak = num

		num, err = strconv.Atoi(parts[3])
		if err != nil {
			fmt.Println("Invalid FailCount")
			return false
		}
		w.FailCount = num

		lastFail, err := time.Parse(crawlItemTimeFormat, parts[4])
		if len(parts[4]) > 0 && err != nil {
			fmt.Println("Invalid LastFailStreak")
			return false
		}

		if err == nil {
			w.LastFailStreak = &lastFail
		} else {
			w.LastFailStreak = nil
		}

		num, err = strconv.Atoi(parts[5])
		if err != nil {
			fmt.Println("Invalid LatestCycle")
			return false
		}
		w.LatestCycle = num

	} else if len(parts) == 5 {
		// Compability without failStreak:
		w.Host = parts[0]
		w.Scheme = parts[1]

		num, err := strconv.Atoi(parts[2])
		if err != nil {
			fmt.Println("Invalid failstreak")
			return false
		}
		// Failstreak resetten voor huidig systeem
		w.FailStreak = 0

		num, err = strconv.Atoi(parts[3])
		if err != nil {
			fmt.Println("Invalid SucceededDownloads")
			return false
		}
		// SucceededDownloads bestaat niet meer

		num, err = strconv.Atoi(parts[4])
		if err != nil {
			fmt.Println("Invalid LatestCycle")
			return false
		}
		w.LatestCycle = num
	} else {
		return false
	}

	// Subdomains
	line, _, _ = reader.ReadLine()
	subdomains := make([]*Subdomain, 0)
	for len(line) > 0 {
		u, err := url.Parse(string(line))
		if err != nil {
			fmt.Println("Fout bij lezen subdomains")
			return false
		}
		subdomain := &Subdomain{Url: u, AlreadyFound: make(map[string]*CrawlItem)}
		w.Subdomains[u.Host] = subdomain
		subdomains = append(subdomains, subdomain)
		line, _, _ = reader.ReadLine()
	}

	w.IntroductionPoints.ReadFromReader(reader, subdomains)
	w.PriorityQueue.ReadFromReader(reader, subdomains)
	w.Queue.ReadFromReader(reader, subdomains)
	w.LowPriorityQueue.ReadFromReader(reader, subdomains)
	w.FailedQueue.ReadFromReader(reader, subdomains)

	// Already visited items
	line, _, _ = reader.ReadLine()
	for len(line) > 0 {
		str = string(line)
		item := NewCrawlItemFromString(&str, subdomains)
		if item == nil {
			fmt.Println("Invalid item: " + str)
		}
		line, _, _ = reader.ReadLine()
	}
	return true
}

func (w *Hostworker) SaveToWriter(writer *bufio.Writer) {
	str := fmt.Sprintf(
		"%s	%s	%v	%v	%s	%v\n",
		w.Host,
		w.Scheme,
		w.FailStreak,
		w.FailCount,
		TimeToString(w.LastFailStreak),
		w.LatestCycle,
	)
	writer.WriteString(str)

	// Subdomains
	i := 0
	for _, subdomain := range w.Subdomains {
		subdomain.Index = i
		// Index wordt hier niet meteen uitgeschreven, maar zal door crawlItem's later uitgeschreven wordne
		// zodat ze de subdomain kunnen terug vinden
		writer.WriteString(subdomain.Url.String())
		writer.WriteString("\n")
		i++
	}
	writer.WriteString("\n")

	w.IntroductionPoints.SaveToWriter(writer)
	w.PriorityQueue.SaveToWriter(writer)
	w.Queue.SaveToWriter(writer)
	w.LowPriorityQueue.SaveToWriter(writer)
	w.FailedQueue.SaveToWriter(writer)

	// Nu de rest opslaan
	for _, subdomain := range w.Subdomains {
		for _, item := range subdomain.AlreadyFound {
			if item.Queue == nil {
				// Staat in geen andere queue
				writer.WriteString(item.SaveToString())
				writer.WriteString("\n")
			}
		}
	}
}

func (w *Hostworker) IsEqual(b *Hostworker) bool {
	if w.Host != b.Host {
		return false
	}

	if w.Scheme != b.Scheme {
		return false
	}

	if w.FailStreak != b.FailStreak {
		fmt.Println("FailStreak wrong")
		return false
	}

	if w.FailCount != b.FailCount {
		fmt.Println("failcount wrong")
		return false
	}

	if !(w.LastFailStreak == nil && b.LastFailStreak == nil) && (w.LastFailStreak == nil || b.LastFailStreak == nil || w.LastFailStreak.Equal(*b.LastFailStreak)) {
		fmt.Println("LastFailStreak wrong")
		return false
	}

	if w.LatestCycle != b.LatestCycle {
		fmt.Println("LatestCycle wrong")
		return false
	}

	if !w.IntroductionPoints.IsEqual(b.IntroductionPoints) {
		return false
	}

	if !w.PriorityQueue.IsEqual(b.PriorityQueue) {
		return false
	}

	if !w.Queue.IsEqual(b.Queue) {
		return false
	}

	if !w.LowPriorityQueue.IsEqual(b.LowPriorityQueue) {
		return false
	}

	if !w.FailedQueue.IsEqual(b.FailedQueue) {
		return false
	}

	if len(w.Subdomains) != len(b.Subdomains) {
		return false
	}

	for key, subdomain := range w.Subdomains {
		otherSub, found := b.Subdomains[key]
		if !found {
			return false
		}

		if len(subdomain.AlreadyFound) != len(otherSub.AlreadyFound) {
			return false
		}

		for key, value := range subdomain.AlreadyFound {
			other, found := otherSub.AlreadyFound[key]
			if !found {
				return false
			}

			if !value.IsEqual(other) {
				return false
			}
		}
	}

	return true
}
