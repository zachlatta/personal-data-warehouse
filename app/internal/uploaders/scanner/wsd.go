package scanner

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/xml"
	"fmt"
	"html"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"strings"
)

// WS-Scan (Microsoft's "Web Services on Devices" scan service) is how the
// pages are pulled: a scan job this computer creates, read page by page. It
// needs no driver and no setting on the scanner. (WS-Scan can also put
// destinations on a panel, but the MFC-L2710DW's panel has no WS Scan entry;
// its button reaches this computer through skey.go instead.)

const (
	nsSOAP     = "http://www.w3.org/2003/05/soap-envelope"
	nsAddress  = "http://schemas.xmlsoap.org/ws/2004/08/addressing"
	nsScan     = "http://schemas.microsoft.com/windows/2006/08/wdp/scan"
	anonymous  = nsAddress + "/role/anonymous"
	actionScan = nsScan + "/"
)

// Fault is a SOAP fault from the scanner, named by its subcode
// ("ClientErrorNoImagesAvailable", "ServerErrorNotAcceptingJobs", ...).
type Fault struct {
	HTTPStatus int
	Subcode    string
	Reason     string
}

func (f *Fault) Error() string {
	return fmt.Sprintf("scanner fault %s (HTTP %d): %s", f.Subcode, f.HTTPStatus, f.Reason)
}

// Is reports whether err is a scanner fault with one of the subcodes.
func isFault(err error, subcodes ...string) bool {
	f, ok := err.(*Fault)
	if !ok {
		return false
	}
	for _, s := range subcodes {
		if f.Subcode == s {
			return true
		}
	}
	return false
}

// Client talks to one scanner's WS-Scan service.
type Client struct {
	// ServiceURL is the scanner service, e.g.
	// http://192.0.2.216:80/WebServices/ScannerService.
	ServiceURL string
	HTTP       *http.Client
}

func newMessageID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("urn:uuid:%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// envelope is a SOAP 1.2 request with WS-Addressing headers; body is raw XML.
func envelope(to, action, body string) []byte {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="utf-8"?>`)
	b.WriteString(`<soap:Envelope xmlns:soap="` + nsSOAP + `" xmlns:wsa="` + nsAddress + `" xmlns:sca="` + nsScan + `">`)
	b.WriteString(`<soap:Header><wsa:To>` + html.EscapeString(to) + `</wsa:To><wsa:Action>` + action + `</wsa:Action>`)
	b.WriteString(`<wsa:MessageID>` + newMessageID() + `</wsa:MessageID>`)
	b.WriteString(`<wsa:ReplyTo><wsa:Address>` + anonymous + `</wsa:Address></wsa:ReplyTo>`)
	b.WriteString(`</soap:Header><soap:Body>` + body + `</soap:Body></soap:Envelope>`)
	return []byte(b.String())
}

// call posts one request and returns the response's SOAP part plus, for an
// MTOM response (RetrieveImage), the attached binary part.
func (c *Client) call(ctx context.Context, action, body string) (soapXML, attachment []byte, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.ServiceURL, bytes.NewReader(envelope(c.ServiceURL, action, body)))
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Content-Type", "application/soap+xml; charset=utf-8")
	httpClient := c.HTTP
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 512<<20))
	if err != nil {
		return nil, nil, err
	}
	soapXML, attachment, err = splitMTOM(resp.Header.Get("Content-Type"), raw)
	if err != nil {
		return nil, nil, err
	}
	if fault := parseFault(soapXML); fault != nil {
		fault.HTTPStatus = resp.StatusCode
		return nil, nil, fault
	}
	if resp.StatusCode/100 != 2 {
		return nil, nil, fmt.Errorf("scanner %s: HTTP %d: %s", action, resp.StatusCode, truncate(soapXML, 300))
	}
	return soapXML, attachment, nil
}

// splitMTOM separates a multipart/related (XOP) response into its SOAP root
// and its one binary attachment. A plain SOAP response is returned as is.
func splitMTOM(contentType string, raw []byte) (soapXML, attachment []byte, err error) {
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil || !strings.EqualFold(mediaType, "multipart/related") {
		return raw, nil, nil
	}
	reader := multipart.NewReader(bytes.NewReader(raw), params["boundary"])
	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("read multipart response: %w", err)
		}
		data, err := io.ReadAll(part)
		if err != nil {
			return nil, nil, fmt.Errorf("read multipart part: %w", err)
		}
		if soapXML == nil && strings.Contains(strings.ToLower(part.Header.Get("Content-Type")), "xml") {
			soapXML = data
			continue
		}
		if attachment == nil {
			attachment = data
		}
	}
	if soapXML == nil {
		return nil, nil, fmt.Errorf("multipart response had no SOAP part")
	}
	return soapXML, attachment, nil
}

func parseFault(soapXML []byte) *Fault {
	var env struct {
		Body struct {
			Fault *struct {
				Code struct {
					Value   string `xml:"Value"`
					Subcode struct {
						Value string `xml:"Value"`
					} `xml:"Subcode"`
				} `xml:"Code"`
				Reason struct {
					Text string `xml:"Text"`
				} `xml:"Reason"`
			} `xml:"Fault"`
		} `xml:"Body"`
	}
	if xml.Unmarshal(soapXML, &env) != nil || env.Body.Fault == nil {
		return nil
	}
	code := env.Body.Fault.Code.Subcode.Value
	if code == "" {
		code = env.Body.Fault.Code.Value
	}
	if i := strings.LastIndexByte(code, ':'); i >= 0 {
		code = code[i+1:]
	}
	return &Fault{Subcode: strings.TrimSpace(code), Reason: strings.TrimSpace(env.Body.Fault.Reason.Text)}
}

// ScannerName is the name the scanner gives itself.
func (c *Client) ScannerName(ctx context.Context) (string, error) {
	body := `<sca:GetScannerElementsRequest><sca:RequestedElements><sca:Name>sca:ScannerDescription</sca:Name></sca:RequestedElements></sca:GetScannerElementsRequest>`
	resp, _, err := c.call(ctx, actionScan+"GetScannerElements", body)
	if err != nil {
		return "", err
	}
	var env struct {
		Body struct {
			Name string `xml:"GetScannerElementsResponse>ScannerElements>ElementData>ScannerDescription>ScannerName"`
		} `xml:"Body"`
	}
	if err := xml.Unmarshal(resp, &env); err != nil {
		return "", fmt.Errorf("parse GetScannerElementsResponse: %w", err)
	}
	return strings.TrimSpace(env.Body.Name), nil
}

// Ticket is what one scan job asks for.
type Ticket struct {
	JobName string
	// Format is "exif" (JPEG) or "tiff-single-g4" (1-bit CCITT G4).
	Format string
	// ColorProcessing is "RGB24", "Grayscale8" or "BlackAndWhite1".
	ColorProcessing string
	Resolution      int
	// InputSource is "ADF" or "Platen".
	InputSource string
	// WidthMils and HeightMils are the scan area in thousandths of an inch.
	WidthMils, HeightMils int
}

// Job is a created scan job.
type Job struct {
	ID    string
	Token string
}

// CreateScanJob starts a job. After a panel press the scanner is waiting
// for exactly this ("Connecting to PC"), and scans what is loaded.
func (c *Client) CreateScanJob(ctx context.Context, t Ticket) (Job, error) {
	var b strings.Builder
	b.WriteString(`<sca:CreateScanJobRequest>`)
	fmt.Fprintf(&b, `<sca:ScanTicket><sca:JobDescription><sca:JobName>%s</sca:JobName><sca:JobOriginatingUserName>pdw</sca:JobOriginatingUserName></sca:JobDescription>`, html.EscapeString(t.JobName))
	fmt.Fprintf(&b, `<sca:DocumentParameters><sca:Format>%s</sca:Format><sca:ImagesToTransfer>0</sca:ImagesToTransfer><sca:InputSource>%s</sca:InputSource>`, t.Format, t.InputSource)
	fmt.Fprintf(&b, `<sca:InputSize><sca:InputMediaSize><sca:Width>%d</sca:Width><sca:Height>%d</sca:Height></sca:InputMediaSize></sca:InputSize>`, t.WidthMils, t.HeightMils)
	fmt.Fprintf(&b, `<sca:MediaSides><sca:MediaFront><sca:ScanRegion><sca:ScanRegionXOffset>0</sca:ScanRegionXOffset><sca:ScanRegionYOffset>0</sca:ScanRegionYOffset><sca:ScanRegionWidth>%d</sca:ScanRegionWidth><sca:ScanRegionHeight>%d</sca:ScanRegionHeight></sca:ScanRegion>`, t.WidthMils, t.HeightMils)
	fmt.Fprintf(&b, `<sca:ColorProcessing>%s</sca:ColorProcessing><sca:Resolution><sca:Width>%d</sca:Width><sca:Height>%d</sca:Height></sca:Resolution></sca:MediaFront></sca:MediaSides>`, t.ColorProcessing, t.Resolution, t.Resolution)
	b.WriteString(`</sca:DocumentParameters></sca:ScanTicket></sca:CreateScanJobRequest>`)
	resp, _, err := c.call(ctx, actionScan+"CreateScanJob", b.String())
	if err != nil {
		return Job{}, err
	}
	var env struct {
		Body struct {
			Response struct {
				JobID    string `xml:"JobId"`
				JobToken string `xml:"JobToken"`
			} `xml:"CreateScanJobResponse"`
		} `xml:"Body"`
	}
	if err := xml.Unmarshal(resp, &env); err != nil {
		return Job{}, fmt.Errorf("parse CreateScanJobResponse: %w", err)
	}
	job := Job{ID: strings.TrimSpace(env.Body.Response.JobID), Token: strings.TrimSpace(env.Body.Response.JobToken)}
	if job.ID == "" {
		return Job{}, fmt.Errorf("CreateScanJobResponse had no JobId: %s", truncate(resp, 300))
	}
	return job, nil
}

// errNoMoreImages ends a job's page loop.
var errNoMoreImages = fmt.Errorf("no more images")

// RetrieveImage pulls the job's next page. It returns errNoMoreImages once
// the job has none left: the scanner says so with
// ClientErrorNoImagesAvailable, or (Brother) by forgetting the job as soon
// as its last page is sent, ClientErrorJobIdNotFound.
func (c *Client) RetrieveImage(ctx context.Context, job Job, page int) ([]byte, error) {
	body := fmt.Sprintf(`<sca:RetrieveImageRequest><sca:JobId>%s</sca:JobId><sca:JobToken>%s</sca:JobToken><sca:DocumentDescription><sca:DocumentName>page%d</sca:DocumentName></sca:DocumentDescription></sca:RetrieveImageRequest>`,
		html.EscapeString(job.ID), html.EscapeString(job.Token), page)
	_, image, err := c.call(ctx, actionScan+"RetrieveImage", body)
	if isFault(err, "ClientErrorNoImagesAvailable", "ClientErrorJobIdNotFound") {
		return nil, errNoMoreImages
	}
	if err != nil {
		return nil, err
	}
	if len(image) == 0 {
		return nil, fmt.Errorf("RetrieveImage returned no image")
	}
	return image, nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func truncate(b []byte, n int) string {
	if len(b) > n {
		return string(b[:n]) + "..."
	}
	return string(b)
}
