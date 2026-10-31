package probe

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/probe/snapshot"
	"github.com/labstack/echo/v4"
)

const (
	maxRequestBody       = 4096
	maxAlarmIDLength     = 128
	maxSnoozeMinutes     = 1440
	defaultAlarmsSince   = 24 * time.Hour
	defaultLogRetention  = 168 * time.Hour
	maxHistoryMinutes    = 10
	defaultHistoryMinute = 10
)

var historyStages = map[string]bool{
	"ingress":   true,
	"validator": true,
	"cache":     true,
	"assembly":  true,
	"chain":     true,
	"stores":    true,
}

type apiError struct {
	Error string `json:"error"`
}

type alarmsResponse struct {
	Since  time.Time        `json:"since"`
	Alarms []snapshot.Alarm `json:"alarms"`
}

type snoozeRequest struct {
	ID      string `json:"id"`
	Minutes int    `json:"minutes"`
}

type historyPoint struct {
	T  time.Time       `json:"t"`
	V  *float64        `json:"v"`
	St snapshot.Status `json:"st"`
}

type historySeries struct {
	Key    string         `json:"key"`
	Points []historyPoint `json:"points"`
}

type historyResponse struct {
	Stage  string          `json:"stage"`
	Series []historySeries `json:"series"`
}

func badRequest(c echo.Context, msg string) error {
	return c.JSON(http.StatusBadRequest, apiError{Error: msg})
}

func notRunning(c echo.Context) error {
	return c.JSON(http.StatusServiceUnavailable, apiError{Error: "probe is not running"})
}

func (s *Server) handleSnapshot(c echo.Context) error {
	p := s.pipe()
	if p == nil {
		return notRunning(c)
	}

	return c.JSON(http.StatusOK, p.Latest())
}

func (s *Server) logRetention() time.Duration {
	if r := s.settings.Probe.AlarmLogRetention; r > 0 {
		return r
	}

	return defaultLogRetention
}

func parseSince(raw string, now time.Time, retention time.Duration) (time.Time, bool) {
	if raw == "" {
		return now.Add(-defaultAlarmsSince), true
	}

	if n, err := strconv.Atoi(raw); err == nil {
		if n < 1 {
			return time.Time{}, false
		}

		if int64(n) > int64(retention/time.Minute) {
			return now.Add(-retention), true
		}

		return now.Add(-time.Duration(n) * time.Minute), true
	}

	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, false
	}

	return t, true
}

func (s *Server) handleAlarms(c echo.Context) error {
	p := s.pipe()
	if p == nil {
		return notRunning(c)
	}

	now := time.Now()
	retention := s.logRetention()

	since, ok := parseSince(c.QueryParam("since"), now, retention)
	if !ok {
		return badRequest(c, "since must be an RFC3339 time or a positive whole number of minutes")
	}

	if oldest := now.Add(-retention); since.Before(oldest) {
		since = oldest
	}

	return c.JSON(http.StatusOK, alarmsResponse{Since: since.UTC(), Alarms: p.store.AlarmsSince(since)})
}

func decodeBody(c echo.Context, dst any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(c.Response(), c.Request().Body, maxRequestBody))

	if err := dec.Decode(dst); err != nil {
		return false
	}

	return errors.Is(dec.Decode(&struct{}{}), io.EOF)
}

func (s *Server) validAlarmID(p *pipeline, id string) bool {
	return id != "" && len(id) <= maxAlarmIDLength && p.knows(id)
}

func (s *Server) handleSnooze(c echo.Context) error {
	p := s.pipe()
	if p == nil {
		return notRunning(c)
	}

	var req snoozeRequest
	if !decodeBody(c, &req) {
		return badRequest(c, "body must be JSON {\"id\": string, \"minutes\": integer}")
	}

	if !s.validAlarmID(p, req.ID) {
		return badRequest(c, "id must name a known alarm")
	}

	if req.Minutes < 0 || req.Minutes > maxSnoozeMinutes {
		return badRequest(c, "minutes must be 0 for the default or from 1 to 1440")
	}

	if err := p.engine.Snooze(req.ID, time.Duration(req.Minutes)*time.Minute); err != nil {
		s.logger.Errorf("[Probe] could not save snooze: %s", oneLine(err))

		return c.JSON(http.StatusInternalServerError, apiError{Error: "could not save snooze"})
	}

	return c.NoContent(http.StatusNoContent)
}

func (s *Server) handleUnsnooze(c echo.Context) error {
	p := s.pipe()
	if p == nil {
		return notRunning(c)
	}

	var req snoozeRequest
	if !decodeBody(c, &req) {
		return badRequest(c, "body must be JSON {\"id\": string}")
	}

	if !s.validAlarmID(p, req.ID) {
		return badRequest(c, "id must name a known alarm")
	}

	if err := p.engine.Unsnooze(req.ID); err != nil {
		s.logger.Errorf("[Probe] could not clear snooze: %s", oneLine(err))

		return c.JSON(http.StatusInternalServerError, apiError{Error: "could not clear snooze"})
	}

	return c.NoContent(http.StatusNoContent)
}

func (s *Server) handleHistory(c echo.Context) error {
	p := s.pipe()
	if p == nil {
		return notRunning(c)
	}

	stage := c.QueryParam("stage")
	if !historyStages[stage] {
		return badRequest(c, "stage must be one of ingress, validator, cache, assembly, chain, stores")
	}

	minutes := defaultHistoryMinute

	if raw := c.QueryParam("minutes"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > maxHistoryMinutes {
			return badRequest(c, "minutes must be a whole number from 1 to 10")
		}

		minutes = n
	}

	history := p.sampler.History(time.Duration(minutes) * time.Minute)
	prefix := stage + "."
	resp := historyResponse{Stage: stage, Series: []historySeries{}}
	index := map[string]int{}

	for i := range history {
		snap := &history[i]

		snap.EachReading(func(key string, r *snapshot.Reading) {
			if !strings.HasPrefix(key, prefix) {
				return
			}

			at, found := index[key]
			if !found {
				at = len(resp.Series)
				index[key] = at
				resp.Series = append(resp.Series, historySeries{Key: key, Points: []historyPoint{}})
			}

			point := historyPoint{T: snap.Time, St: r.Status}

			if r.Status != snapshot.StatusNoData {
				v := r.Value
				point.V = &v
			}

			resp.Series[at].Points = append(resp.Series[at].Points, point)
		})
	}

	return c.JSON(http.StatusOK, resp)
}
