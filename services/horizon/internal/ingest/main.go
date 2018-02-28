// Package ingest contains the ingestion system for horizon.  This system takes
// data produced by the connected stellar-core database, transforms it and
// inserts it into the horizon database.
package ingest

import (
	"sync"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/guregu/null"
	metrics "github.com/rcrowley/go-metrics"
	"github.com/stellar/go/services/horizon/internal/db2/core"
	"github.com/stellar/go/services/horizon/internal/db2/history"
	"github.com/stellar/go/support/db"
	"github.com/stellar/go/xdr"
)

const (
	// CurrentVersion reflects the latest version of the ingestion
	// algorithm. As rows are ingested into the horizon database, this version is
	// used to tag them.  In the future, any breaking changes introduced by a
	// developer should be accompanied by an increase in this value.
	//
	// Scripts, that have yet to be ported to this codebase can then be leveraged
	// to re-ingest old data with the new algorithm, providing a seamless
	// transition when the ingested data's structure changes.
	CurrentVersion = 11
)

// Cursor iterates through a stellar core database's ledgers
type Cursor struct {
	// FirstLedger is the beginning of the range of ledgers (inclusive) that will
	// attempt to be ingested in this session.
	FirstLedger int32
	// LastLedger is the end of the range of ledgers (inclusive) that will
	// attempt to be ingested in this session.
	LastLedger int32
	// DB is the stellar-core db that data is ingested from.
	DB *db.Session

	Metrics        *IngesterMetrics
	AssetsModified AssetsModified

	// Err is the error that caused this iteration to fail, if any.
	Err error

	lg   int32
	tx   int
	op   int
	data *LedgerBundle
}

// EffectIngestion is a helper struct to smooth the ingestion of effects.  this
// struct will track what the correct operation to use and order to use when
// adding effects into an ingestion.
type EffectIngestion struct {
	Dest        *Ingestion
	OperationID int64
	err         error
	added       int
	parent      *Ingestion
}

// LedgerBundle represents a single ledger's worth of novelty created by one
// ledger close
type LedgerBundle struct {
	Sequence        int32
	Header          core.LedgerHeader
	TransactionFees []core.TransactionFee
	Transactions    []core.Transaction
}

// System represents the data ingestion subsystem of horizon.
type System struct {
	// HorizonDB is the connection to the horizon database that ingested data will
	// be written to.
	HorizonDB *db.Session

	// CoreDB is the stellar-core db that data is ingested from.
	CoreDB *db.Session

	Metrics IngesterMetrics

	// Network is the passphrase for the network being imported
	Network string

	// StellarCoreURL is the http endpoint of the stellar-core that data is being
	// ingested from.
	StellarCoreURL string

	// SkipCursorUpdate causes the ingestor to skip
	// reporting the "last imported ledger" cursor to
	// stellar-core
	SkipCursorUpdate bool

	// HistoryRetentionCount is the desired minimum number of ledgers to
	// keep in the history database, working backwards from the latest core
	// ledger.  0 represents "all ledgers".
	HistoryRetentionCount uint

	lock    sync.Mutex
	current *Session
}

// IngesterMetrics tracks all the metrics for the ingestion subsystem
type IngesterMetrics struct {
	ClearLedgerTimer  metrics.Timer
	IngestLedgerTimer metrics.Timer
	LoadLedgerTimer   metrics.Timer
}

// AssetsModified tracks all the assets modified during a cycle of ingestion
type AssetsModified map[string]xdr.Asset

type TableName string

const (
	EffectsTableName                 TableName = "history_effects"
	LedgersTableName                 TableName = "history_ledgers"
	OperationParticipantsTableName   TableName = "history_operation_participants"
	OperationsTableName              TableName = "history_operations"
	TradesTableName                  TableName = "history_trades"
	TransactionParticipantsTableName TableName = "history_transaction_participants"
	TransactionsTableName            TableName = "history_transactions"
)

// row should be implemented by objects added to DB during ingestion.
type row interface {
	// GetParams returns fields to be added to DB. Objects can contain
	// more helper fields that are not added to DB.
	GetParams() []interface{}
	// UpdateAccountIDs updates fields with account IDs by using provided
	// address => id mapping.
	UpdateAccountIDs(accounts map[string]int64)
	// GetAddresses returns a list of addresses to find corresponding IDs
	GetAddresses() []string
	GetTableName() TableName
}

type effectRow struct {
	AccountID   int64
	OperationID int64
	Order       int
	Type        history.EffectType
	Details     []byte

	Address string
}

type operationRow struct {
	ID      int64
	TxID    int64
	Order   int32
	Source  string
	Type    xdr.OperationType
	Details []byte
}

type operationParticipantRow struct {
	OperationID int64
	AccountID   int64

	Address string
}

type ledgerRow struct {
	ImporterVersion    int32
	ID                 int64
	Sequence           uint32
	LedgerHash         string
	PreviousLedgerHash null.String
	TotalCoins         int64
	FeePool            int64
	BaseFee            int32
	BaseReserve        int32
	MaxTxSetSize       int32
	ClosedAt           time.Time
	CreatedAt          time.Time
	UpdatedAt          time.Time
	TransactionCount   int32
	OperationCount     int32
	ProtocolVersion    int32
	LedgerHeaderXDR    null.String
}

type tradeRow struct {
	OperationID      int64
	Order            int32
	LedgerCloseAt    time.Time
	OfferID          xdr.Uint64
	BaseAccountID    int64
	BaseAssetID      int64
	BaseAmount       xdr.Int64
	CounterAccountID int64
	CounterAssetID   int64
	CounterAmount    xdr.Int64
	BaseIsSeller     bool

	BaseAddress    string
	CounterAddress string
}

type transactionRow struct {
	ID               int64
	TransactionHash  string
	LedgerSequence   int32
	ApplicationOrder int32
	Account          string
	AccountSequence  int64
	FeePaid          int32
	OperationCount   int
	TxEnvelope       string
	TxResult         string
	TxMeta           string
	TxFeeMeta        string
	SignaturesString interface{}
	TimeBounds       interface{}
	MemoType         string
	Memo             null.String
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

type transactionParticipantRow struct {
	TransactionID int64
	AccountID     int64

	Address string
}

// Ingestion receives write requests from a Session
type Ingestion struct {
	// DB is the sql connection to be used for writing any rows into the horizon
	// database.
	DB *db.Session

	builders     map[TableName]sq.InsertBuilder
	rowsToInsert []row
	assetStats   sq.InsertBuilder
}

// Session represents a single attempt at ingesting data into the history
// database.
type Session struct {
	Cursor    *Cursor
	Ingestion *Ingestion
	// Network is the passphrase for the network being imported
	Network string

	// StellarCoreURL is the http endpoint of the stellar-core that data is being
	// ingested from.
	StellarCoreURL string

	// ClearExisting causes the session to clear existing data from the horizon db
	// when the session is run.
	ClearExisting bool

	// SkipCursorUpdate causes the session to skip
	// reporting the "last imported ledger" cursor to
	// stellar-core
	SkipCursorUpdate bool

	// Metrics is a reference to where the session should record its metric information
	Metrics *IngesterMetrics

	//
	// Results fields
	//

	// Err is the error that caused this session to fail, if any.
	Err error

	// Ingested is the number of ledgers that were successfully ingested during
	// this session.
	Ingested int
}

// New initializes the ingester, causing it to begin polling the stellar-core
// database for now ledgers and ingesting data into the horizon database.
func New(network string, coreURL string, core, horizon *db.Session) *System {
	i := &System{
		Network:        network,
		StellarCoreURL: coreURL,
		HorizonDB:      horizon,
		CoreDB:         core,
	}

	i.Metrics.ClearLedgerTimer = metrics.NewTimer()
	i.Metrics.IngestLedgerTimer = metrics.NewTimer()
	i.Metrics.LoadLedgerTimer = metrics.NewTimer()
	return i
}

// NewCursor initializes a new ingestion cursor
func NewCursor(first, last int32, i *System) *Cursor {
	return &Cursor{
		FirstLedger:    first,
		LastLedger:     last,
		DB:             i.CoreDB,
		Metrics:        &i.Metrics,
		AssetsModified: AssetsModified(make(map[string]xdr.Asset)),
	}
}

// NewSession initialize a new ingestion session
func NewSession(i *System) *Session {
	hdb := i.HorizonDB.Clone()

	return &Session{
		Ingestion: &Ingestion{
			DB: hdb,
		},
		Network:          i.Network,
		StellarCoreURL:   i.StellarCoreURL,
		SkipCursorUpdate: i.SkipCursorUpdate,
		Metrics:          &i.Metrics,
	}
}
