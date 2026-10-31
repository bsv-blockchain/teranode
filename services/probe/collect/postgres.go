package collect

import (
	"context"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/probe/snapshot"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	postgresBytesPerGB    = 1e9
	postgresMsPerSec      = 1000.0
	postgresNoCapacity    = "disk capacity not set"
	postgresNoStatements  = "needs pg_stat_statements"
	postgresMeanNote      = "mean"
	postgresApplication   = "teranode-probe"
	postgresCloseBound    = time.Second
	postgresStatementsExt = "pg_stat_statements"
)

var postgresPassThroughParams = []string{"sslmode", "sslrootcert", "sslcert", "sslkey", "connect_timeout", "application_name"}

type Postgres struct {
	connString string
	timeout    time.Duration
	capacityGB float64
	now        func() time.Time

	mu       sync.Mutex
	conn     *pgx.Conn
	closed   bool
	previous *postgresSample
}

type postgresSample struct {
	at           time.Time
	commits      int64
	readRows     int64
	writeRows    int64
	execMs       float64
	calls        int64
	hasStatement bool
}

func NewPostgres(u *url.URL, timeout time.Duration, diskCapacityGB float64) (*Postgres, error) {
	if u == nil {
		return &Postgres{timeout: timeout, now: time.Now}, nil
	}

	if timeout <= 0 {
		return nil, errors.NewConfigurationError("probe postgres collector needs a positive timeout")
	}

	if diskCapacityGB < 0 {
		return nil, errors.NewConfigurationError("probe postgres disk capacity must not be negative")
	}

	database := strings.TrimPrefix(u.Path, "/")
	if database == "" {
		return nil, errors.NewConfigurationError("probe postgres url has no database name")
	}

	if u.Hostname() == "" {
		return nil, errors.NewConfigurationError("probe postgres url has no host")
	}

	user, password := "", ""
	if u.User != nil {
		user = u.User.Username()
		password, _ = u.User.Password()
	}

	parts := []string{
		"user=" + quotePostgresValue(user),
		"password=" + quotePostgresValue(password),
		"dbname=" + quotePostgresValue(database),
		"host=" + quotePostgresValue(u.Hostname()),
	}

	query := u.Query()

	for _, key := range postgresPassThroughParams {
		if v := query.Get(key); v != "" {
			parts = append(parts, key+"="+quotePostgresValue(v))
		}
	}

	if port := u.Port(); port != "" {
		if _, err := strconv.ParseUint(port, 10, 16); err != nil {
			return nil, errors.NewConfigurationError("probe postgres url has an invalid port")
		}

		parts = append(parts, "port="+port)
	}

	p := &Postgres{
		connString: strings.Join(parts, " "),
		timeout:    timeout,
		capacityGB: diskCapacityGB,
		now:        time.Now,
	}

	if _, err := pgx.ParseConfig(p.connString); err != nil {
		return nil, errors.NewConfigurationError("probe postgres url is invalid")
	}

	return p, nil
}

func quotePostgresValue(value string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(value) + "'"
}

func PostgresURL(s *settings.Settings) *url.URL {
	if s == nil {
		return nil
	}

	if s.Probe.PostgresURL != nil {
		return s.Probe.PostgresURL
	}

	if u := s.BlockChain.StoreURL; u != nil && u.Scheme == "postgres" {
		return u
	}

	return nil
}

func (p *Postgres) Name() string {
	return "postgres"
}

func (p *Postgres) connConfig() *pgx.ConnConfig {
	cfg, err := pgx.ParseConfig(p.connString)
	if err != nil {
		return nil
	}

	if cfg.ConnectTimeout <= 0 || cfg.ConnectTimeout > p.timeout {
		cfg.ConnectTimeout = p.timeout
	}

	cfg.RuntimeParams["default_transaction_read_only"] = "on"
	if _, set := cfg.RuntimeParams["application_name"]; !set {
		cfg.RuntimeParams["application_name"] = postgresApplication
	}

	return cfg
}

func (p *Postgres) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.closed = true
	p.dropConn()
}

func (p *Postgres) dropConn() {
	if p.conn == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), min(p.timeout, postgresCloseBound))
	defer cancel()

	_ = p.conn.Close(ctx)
	p.conn = nil
}

func (p *Postgres) connection(ctx context.Context) (*pgx.Conn, error) {
	if p.closed {
		return nil, errors.NewServiceError("probe postgres collector is closed")
	}

	if p.conn != nil {
		return p.conn, nil
	}

	conn, err := pgx.ConnectConfig(ctx, p.connConfig())
	if err != nil {
		return nil, errors.NewServiceError("could not connect probe to postgres", err)
	}

	p.conn = conn

	return conn, nil
}

func notConfiguredPostgresPatch(s *snapshot.Snapshot) {
	st := &s.Stores.Postgres
	st.Configured = false
	st.DiskFill = snapshot.Reading{Status: snapshot.StatusNoData}
	st.SizeGB = snapshot.Reading{Status: snapshot.StatusNoData}
	st.ReadP99Sec = snapshot.Reading{Status: snapshot.StatusNoData}
	st.ReadsPerSec = snapshot.Reading{Status: snapshot.StatusNoData}
	st.WritesPerSec = snapshot.Reading{Status: snapshot.StatusNoData}
	st.CommitsPerSec = snapshot.Reading{Status: snapshot.StatusNoData}
}

func (p *Postgres) Collect(ctx context.Context) (snapshot.Patch, error) {
	if p.connString == "" {
		return notConfiguredPostgresPatch, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	conn, err := p.connection(ctx)
	if err != nil {
		return nil, err
	}

	sizeBytes, current, err := readPostgres(ctx, conn)
	if err != nil {
		p.dropConn()
		return nil, err
	}

	current.at = p.now()
	previous := p.previous
	p.previous = current

	sizeGB := float64(sizeBytes) / postgresBytesPerGB

	readings := struct {
		size, fill, p99, reads, writes, commits snapshot.Reading
	}{
		size:    snapshot.Reading{Value: sizeGB, Status: snapshot.StatusOK},
		fill:    diskFillReading(sizeGB, p.capacityGB),
		p99:     snapshot.Reading{Status: snapshot.StatusNoData, Note: postgresNoStatements},
		reads:   snapshot.Reading{Status: snapshot.StatusNoData},
		writes:  snapshot.Reading{Status: snapshot.StatusNoData},
		commits: snapshot.Reading{Status: snapshot.StatusNoData},
	}

	if current.hasStatement {
		readings.p99.Note = ""
	}

	if previous != nil {
		elapsed := current.at.Sub(previous.at).Seconds()

		if v, ok := counterRate(previous.commits, current.commits, elapsed); ok {
			readings.commits = snapshot.Reading{Value: v, Status: snapshot.StatusOK}
		}

		if v, ok := counterRate(previous.readRows, current.readRows, elapsed); ok {
			readings.reads = snapshot.Reading{Value: v, Status: snapshot.StatusOK}
		}

		if v, ok := counterRate(previous.writeRows, current.writeRows, elapsed); ok {
			readings.writes = snapshot.Reading{Value: v, Status: snapshot.StatusOK}
		}

		if previous.hasStatement && current.hasStatement {
			if v, ok := meanExecSeconds(previous.execMs, current.execMs, previous.calls, current.calls); ok {
				readings.p99 = snapshot.Reading{Value: v, Status: snapshot.StatusOK, Note: postgresMeanNote}
			}
		}
	}

	return func(s *snapshot.Snapshot) {
		st := &s.Stores.Postgres
		st.Configured = true
		st.SizeGB = readings.size
		st.DiskFill = readings.fill
		st.ReadP99Sec = readings.p99
		st.ReadsPerSec = readings.reads
		st.WritesPerSec = readings.writes
		st.CommitsPerSec = readings.commits
	}, nil
}

func diskFillReading(sizeGB float64, capacityGB float64) snapshot.Reading {
	if capacityGB <= 0 {
		return snapshot.Reading{Status: snapshot.StatusNoData, Note: postgresNoCapacity}
	}

	return snapshot.Reading{Value: min(max(sizeGB/capacityGB, 0), 1), Status: snapshot.StatusOK}
}

func counterRate(previous int64, current int64, elapsedSec float64) (float64, bool) {
	if elapsedSec <= 0 || current < previous {
		return 0, false
	}

	return float64(current-previous) / elapsedSec, true
}

func meanExecSeconds(previousMs float64, currentMs float64, previousCalls int64, currentCalls int64) (float64, bool) {
	if currentCalls <= previousCalls || currentMs < previousMs {
		return 0, false
	}

	return (currentMs - previousMs) / float64(currentCalls-previousCalls) / postgresMsPerSec, true
}

func readPostgres(ctx context.Context, conn *pgx.Conn) (int64, *postgresSample, error) {
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return 0, nil, errors.NewServiceError("could not start probe postgres read transaction", err)
	}

	size, sample, err := readPostgresStats(ctx, tx)

	if rerr := tx.Rollback(ctx); rerr != nil && err == nil {
		err = errors.NewServiceError("could not end probe postgres read transaction", rerr)
	}

	if err != nil {
		return 0, nil, err
	}

	return size, sample, nil
}

func readPostgresStats(ctx context.Context, tx pgx.Tx) (int64, *postgresSample, error) {
	var (
		size   int64
		sample postgresSample
	)

	err := tx.QueryRow(ctx, `SELECT pg_database_size(current_database()), d.xact_commit,
		d.tup_returned + d.tup_fetched, d.tup_inserted + d.tup_updated + d.tup_deleted
		FROM pg_stat_database d WHERE d.datname = current_database()`).Scan(&size, &sample.commits, &sample.readRows, &sample.writeRows)
	if err != nil {
		return 0, nil, errors.NewServiceError("could not read postgres database statistics", err)
	}

	var installed bool

	err = tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = '"+postgresStatementsExt+"')").Scan(&installed)
	if err != nil {
		return 0, nil, errors.NewServiceError("could not check postgres extensions", err)
	}

	if !installed {
		return size, &sample, nil
	}

	column, err := statementsTimeColumn(ctx, tx)
	if err != nil {
		return 0, nil, err
	}

	if column == "" {
		return size, &sample, nil
	}

	err = tx.QueryRow(ctx, "SELECT COALESCE(sum("+column+"), 0)::float8, COALESCE(sum(calls), 0)::bigint FROM "+postgresStatementsExt+
		" WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())").Scan(&sample.execMs, &sample.calls)
	if err != nil {
		if isPostgresServerError(err) {
			return size, &sample, nil
		}

		return 0, nil, errors.NewServiceError("could not read postgres statement statistics", err)
	}

	sample.hasStatement = true

	return size, &sample, nil
}

func statementsTimeColumn(ctx context.Context, tx pgx.Tx) (string, error) {
	rows, err := tx.Query(ctx, "SELECT attname FROM pg_attribute WHERE attrelid = '"+postgresStatementsExt+"'::regclass AND attname IN ('total_exec_time', 'total_time')")
	if err != nil {
		if isPostgresServerError(err) {
			return "", nil
		}

		return "", errors.NewServiceError("could not inspect postgres statement columns", err)
	}

	defer rows.Close()

	column := ""

	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			return "", errors.NewServiceError("could not inspect postgres statement columns", err)
		}

		if name == "total_exec_time" || column == "" {
			column = name
		}
	}

	if err = rows.Err(); err != nil {
		if isPostgresServerError(err) {
			return "", nil
		}

		return "", errors.NewServiceError("could not inspect postgres statement columns", err)
	}

	return column, nil
}

func isPostgresServerError(err error) bool {
	var pgErr *pgconn.PgError

	return errors.As(err, &pgErr)
}
