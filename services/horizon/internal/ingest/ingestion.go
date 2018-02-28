package ingest

import (
	"encoding/json"
	"fmt"
	"time"

	"math"

	sq "github.com/Masterminds/squirrel"
	"github.com/guregu/null"
	"github.com/stellar/go/services/horizon/internal/db2/core"
	"github.com/stellar/go/services/horizon/internal/db2/history"
	"github.com/stellar/go/services/horizon/internal/db2/sqx"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
)

// ClearAll clears the entire history database
func (ingest *Ingestion) ClearAll() error {
	return ingest.Clear(0, math.MaxInt64)
}

// Clear removes a range of data from the history database, exclusive of the end
// id provided.
func (ingest *Ingestion) Clear(start int64, end int64) error {
	clear := ingest.DB.DeleteRange

	err := clear(start, end, "history_effects", "history_operation_id")
	if err != nil {
		return err
	}
	err = clear(start, end, "history_operation_participants", "history_operation_id")
	if err != nil {
		return err
	}
	err = clear(start, end, "history_operations", "id")
	if err != nil {
		return err
	}
	err = clear(start, end, "history_transaction_participants", "history_transaction_id")
	if err != nil {
		return err
	}
	err = clear(start, end, "history_transactions", "id")
	if err != nil {
		return err
	}
	err = clear(start, end, "history_ledgers", "id")
	if err != nil {
		return err
	}
	err = clear(start, end, "history_trades", "history_operation_id")
	if err != nil {
		return err
	}
	err = clear(start, end, "asset_stats", "id")
	if err != nil {
		return err
	}

	return nil
}

// Close finishes the current transaction and finishes this ingestion.
func (ingest *Ingestion) Close() error {
	return ingest.commit()
}

// Effect adds a new row into the `history_effects` table.
func (ingest *Ingestion) Effect(address string, opid int64, order int, typ history.EffectType, details interface{}) error {
	djson, err := json.Marshal(details)
	if err != nil {
		return err
	}

	effect := &effectRow{
		Address:     address,
		OperationID: opid,
		Order:       order,
		Type:        typ,
		Details:     djson,
	}
	ingest.rowsToInsert = append(ingest.rowsToInsert, effect)

	return nil
}

// Flush writes the currently buffered rows to the db, and if successful
// starts a new transaction.
func (ingest *Ingestion) Flush() error {
	// Update IDs for accounts
	err := ingest.UpdateAccountIDs()
	if err != nil {
		return errors.Wrap(err, "Error while updating account ids")
	}

	// Inserts
	paramsCount := map[TableName]int{}
	for _, row := range ingest.rowsToInsert {
		tableName := row.GetTableName()
		params := row.GetParams()

		if _, ok := ingest.builders[tableName]; !ok {
			return errors.Errorf("%s insert builder does not exist", tableName)
		}

		ingest.builders[tableName] = ingest.builders[tableName].Values(params...)
		paramsCount[tableName] += len(params)

		// PostgreSQL supports up to 65535 parameters.
		if paramsCount[tableName] > 65000 {
			_, err = ingest.DB.Exec(ingest.builders[tableName])
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("Error adding values while inserting to %s", tableName))
			}
			paramsCount[tableName] = 0
			ingest.createInsertBuilderByTableName(tableName)
		}
	}

	// Exec the rest
	for tableName, params := range paramsCount {
		if params > 0 {
			_, err = ingest.DB.Exec(ingest.builders[tableName])
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("Error adding values while inserting to %s", tableName))
			}
		}
	}

	err = ingest.commit()
	if err != nil {
		return err
	}

	return ingest.Start()
}

// Ledger adds a ledger to the current ingestion
func (ingest *Ingestion) Ledger(
	id int64,
	header *core.LedgerHeader,
	txs int,
	ops int,
) {

	ledger := ledgerRow{
		ImporterVersion:    CurrentVersion,
		ID:                 id,
		Sequence:           header.Sequence,
		LedgerHash:         header.LedgerHash,
		PreviousLedgerHash: null.NewString(header.PrevHash, header.Sequence > 1),
		TotalCoins:         int64(header.Data.TotalCoins),
		FeePool:            int64(header.Data.FeePool),
		BaseFee:            int32(header.Data.BaseFee),
		BaseReserve:        int32(header.Data.BaseReserve),
		MaxTxSetSize:       int32(header.Data.MaxTxSetSize),
		ClosedAt:           time.Unix(header.CloseTime, 0).UTC(),
		CreatedAt:          time.Now().UTC(),
		UpdatedAt:          time.Now().UTC(),
		TransactionCount:   int32(txs),
		OperationCount:     int32(ops),
		ProtocolVersion:    int32(header.Data.LedgerVersion),
		LedgerHeaderXDR:    null.StringFrom(header.DataXDR()),
	}
	ingest.rowsToInsert = append(ingest.rowsToInsert, ledger)
}

// Operation ingests the provided operation data into a new row in the
// `history_operations` table
func (ingest *Ingestion) Operation(
	id int64,
	txid int64,
	order int32,
	source xdr.AccountId,
	typ xdr.OperationType,
	details map[string]interface{},

) error {
	djson, err := json.Marshal(details)
	if err != nil {
		return err
	}

	operation := operationRow{
		ID:      id,
		TxID:    txid,
		Order:   order,
		Source:  source.Address(),
		Type:    typ,
		Details: djson,
	}
	ingest.rowsToInsert = append(ingest.rowsToInsert, operation)
	return nil
}

// UpdateAccountIDs updates IDs of the accounts before inserting
// objects into DB.
func (ingest *Ingestion) UpdateAccountIDs() error {
	// address => ID in DB
	accounts := map[string]int64{}
	addresses := []string{}

	// Collect addresses to fetch
	for _, row := range ingest.rowsToInsert {
		for _, address := range row.GetAddresses() {
			if _, exists := accounts[address]; !exists {
				addresses = append(addresses, address)
			}
			accounts[address] = 0
		}
	}

	if len(addresses) == 0 {
		return nil
	}

	// Get IDs and update map
	q := history.Q{Session: ingest.DB}
	dbAccounts := make([]history.Account, 0, len(addresses))
	err := q.AccountsByAddresses(&dbAccounts, addresses)
	if err != nil {
		return err
	}

	for _, row := range dbAccounts {
		accounts[row.Address] = row.ID
	}

	// Insert non-existent addresses and update map
	addresses = []string{}
	for address, id := range accounts {
		if id == 0 {
			addresses = append(addresses, address)
		}
	}

	if len(addresses) > 0 {
		dbAccounts = make([]history.Account, 0, len(addresses))
		err = q.CreateAccounts(&dbAccounts, addresses)
		if err != nil {
			return err
		}

		for _, row := range dbAccounts {
			accounts[row.Address] = row.ID
		}
	}

	// Update IDs in objects
	for _, row := range ingest.rowsToInsert {
		row.UpdateAccountIDs(accounts)
	}

	return nil
}

// OperationParticipants ingests the provided accounts `aids` as participants of
// operation with id `op`, creating a new row in the
// `history_operation_participants` table.
func (ingest *Ingestion) OperationParticipants(op int64, aids []xdr.AccountId) error {
	for _, aid := range aids {
		operationParticipant := &operationParticipantRow{
			OperationID: op,
			Address:     aid.Address(),
		}
		ingest.rowsToInsert = append(ingest.rowsToInsert, operationParticipant)
	}

	return nil
}

// Rollback aborts this ingestions transaction
func (ingest *Ingestion) Rollback() (err error) {
	err = ingest.DB.Rollback()
	return
}

// Start makes the ingestion reeady, initializing the insert builders and tx
func (ingest *Ingestion) Start() (err error) {
	err = ingest.DB.Begin()
	if err != nil {
		return
	}

	// We need to recreate builders and clear `rowsToInsert` because `Ingestion`
	// object can be used to ingest more than one ledger.
	ingest.createInsertBuilders()
	ingest.rowsToInsert = []row{}
	return
}

// Trade records a trade into the history_trades table
func (ingest *Ingestion) Trade(
	opid int64,
	order int32,
	buyer xdr.AccountId,
	trade xdr.ClaimOfferAtom,
	ledgerClosedAt int64,
) error {
	q := history.Q{Session: ingest.DB}

	sellerAddress := trade.SellerId.Address()
	buyerAddress := buyer.Address()

	soldAssetId, err := q.GetCreateAssetID(trade.AssetSold)
	if err != nil {
		return errors.Wrap(err, "failed to get sold asset id")
	}

	boughtAssetId, err := q.GetCreateAssetID(trade.AssetBought)
	if err != nil {
		return errors.Wrap(err, "failed to get bought asset id")
	}
	var baseAssetId, counterAssetId int64
	var baseAddress, counterAddress string
	var baseAmount, counterAmount xdr.Int64

	//map seller and buyer to base and counter based on ordering of ids
	if soldAssetId < boughtAssetId {
		baseAddress, baseAssetId, baseAmount, counterAddress, counterAssetId, counterAmount =
			sellerAddress, soldAssetId, trade.AmountSold, buyerAddress, boughtAssetId, trade.AmountBought
	} else {
		baseAddress, baseAssetId, baseAmount, counterAddress, counterAssetId, counterAmount =
			buyerAddress, boughtAssetId, trade.AmountBought, sellerAddress, soldAssetId, trade.AmountSold
	}

	tradeR := &tradeRow{
		OperationID:    opid,
		Order:          order,
		LedgerCloseAt:  time.Unix(ledgerClosedAt, 0).UTC(),
		OfferID:        trade.OfferId,
		BaseAssetID:    baseAssetId,
		BaseAmount:     baseAmount,
		CounterAssetID: counterAssetId,
		CounterAmount:  counterAmount,
		BaseIsSeller:   soldAssetId < boughtAssetId,

		BaseAddress:    baseAddress,
		CounterAddress: counterAddress,
	}
	ingest.rowsToInsert = append(ingest.rowsToInsert, tradeR)
	return nil
}

// Transaction ingests the provided transaction data into a new row in the
// `history_transactions` table
func (ingest *Ingestion) Transaction(
	id int64,
	tx *core.Transaction,
	fee *core.TransactionFee,
) {

	signatures := tx.Base64Signatures()

	transaction := transactionRow{
		ID:               id,
		TransactionHash:  tx.TransactionHash,
		LedgerSequence:   tx.LedgerSequence,
		ApplicationOrder: tx.Index,
		Account:          tx.SourceAddress(),
		AccountSequence:  tx.Sequence(),
		FeePaid:          tx.Fee(),
		OperationCount:   len(tx.Envelope.Tx.Operations),
		TxEnvelope:       tx.EnvelopeXDR(),
		TxResult:         tx.ResultXDR(),
		TxMeta:           tx.ResultMetaXDR(),
		TxFeeMeta:        fee.ChangesXDR(),
		SignaturesString: sqx.StringArray(signatures),
		TimeBounds:       ingest.formatTimeBounds(tx.Envelope.Tx.TimeBounds),
		MemoType:         tx.MemoType(),
		Memo:             tx.Memo(),
		CreatedAt:        time.Now().UTC(),
		UpdatedAt:        time.Now().UTC(),
	}
	ingest.rowsToInsert = append(ingest.rowsToInsert, transaction)
}

// TransactionParticipants ingests the provided account ids as participants of
// transaction with id `tx`, creating a new row in the
// `history_transaction_participants` table.
func (ingest *Ingestion) TransactionParticipants(tx int64, aids []xdr.AccountId) error {
	for _, aid := range aids {
		transactionParticipant := &transactionParticipantRow{
			TransactionID: tx,
			Address:       aid.Address(),
		}
		ingest.rowsToInsert = append(ingest.rowsToInsert, transactionParticipant)
	}

	return nil
}

func (ingest *Ingestion) createOperationsInsertBuilder() {
	ingest.builders[OperationsTableName] = sq.Insert(string(OperationsTableName)).Columns(
		"id",
		"transaction_id",
		"application_order",
		"source_account",
		"type",
		"details",
	)
}

func (ingest *Ingestion) createOperationParticipantsInsertBuilder() {
	ingest.builders[OperationParticipantsTableName] = sq.Insert(string(OperationParticipantsTableName)).Columns(
		"history_operation_id",
		"history_account_id",
	)
}

func (ingest *Ingestion) createTransactionParticipantsInsertBuilder() {
	ingest.builders[TransactionParticipantsTableName] = sq.Insert(string(TransactionParticipantsTableName)).Columns(
		"history_transaction_id",
		"history_account_id",
	)
}

func (ingest *Ingestion) createEffectsInsertBuilder() {
	ingest.builders[EffectsTableName] = sq.Insert(string(EffectsTableName)).Columns(
		"history_account_id",
		"history_operation_id",
		"\"order\"",
		"type",
		"details",
	)
}

func (ingest *Ingestion) createTradesInsertBuilder() {
	ingest.builders[TradesTableName] = sq.Insert(string(TradesTableName)).Columns(
		"history_operation_id",
		"\"order\"",
		"ledger_closed_at",
		"offer_id",
		"base_account_id",
		"base_asset_id",
		"base_amount",
		"counter_account_id",
		"counter_asset_id",
		"counter_amount",
		"base_is_seller",
	)
}

func (ingest *Ingestion) createLedgersInsertBuilder() {
	ingest.builders[LedgersTableName] = sq.Insert("history_ledgers").Columns(
		"importer_version",
		"id",
		"sequence",
		"ledger_hash",
		"previous_ledger_hash",
		"total_coins",
		"fee_pool",
		"base_fee",
		"base_reserve",
		"max_tx_set_size",
		"closed_at",
		"created_at",
		"updated_at",
		"transaction_count",
		"operation_count",
		"protocol_version",
		"ledger_header",
	)
}

func (ingest *Ingestion) createTransactionsInsertBuilder() {
	ingest.builders[TransactionsTableName] = sq.Insert("history_transactions").Columns(
		"id",
		"transaction_hash",
		"ledger_sequence",
		"application_order",
		"account",
		"account_sequence",
		"fee_paid",
		"operation_count",
		"tx_envelope",
		"tx_result",
		"tx_meta",
		"tx_fee_meta",
		"signatures",
		"time_bounds",
		"memo_type",
		"memo",
		"created_at",
		"updated_at",
	)
}

func (ingest *Ingestion) createInsertBuilderByTableName(name TableName) {
	switch name {
	case OperationsTableName:
		ingest.createOperationsInsertBuilder()
	case EffectsTableName:
		ingest.createEffectsInsertBuilder()
	case LedgersTableName:
		ingest.createLedgersInsertBuilder()
	case TradesTableName:
		ingest.createTradesInsertBuilder()
	case OperationParticipantsTableName:
		ingest.createOperationParticipantsInsertBuilder()
	case TransactionsTableName:
		ingest.createTransactionsInsertBuilder()
	case TransactionParticipantsTableName:
		ingest.createTransactionParticipantsInsertBuilder()
	default:
		panic("Invalid table name")
	}
}

func (ingest *Ingestion) createInsertBuilders() {
	ingest.builders = make(map[TableName]sq.InsertBuilder)

	ingest.createEffectsInsertBuilder()
	ingest.createLedgersInsertBuilder()
	ingest.createOperationParticipantsInsertBuilder()
	ingest.createOperationsInsertBuilder()
	ingest.createTradesInsertBuilder()
	ingest.createTransactionParticipantsInsertBuilder()
	ingest.createTransactionsInsertBuilder()

	ingest.assetStats = sq.Insert("asset_stats").Columns(
		"id",
		"amount",
		"num_accounts",
		"flags",
		"toml",
	)
}

func (ingest *Ingestion) commit() error {
	err := ingest.DB.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (ingest *Ingestion) formatTimeBounds(bounds *xdr.TimeBounds) interface{} {
	if bounds == nil {
		return nil
	}

	if bounds.MaxTime == 0 {
		return sq.Expr("?::int8range", fmt.Sprintf("[%d,]", bounds.MinTime))
	}

	return sq.Expr("?::int8range", fmt.Sprintf("[%d,%d]", bounds.MinTime, bounds.MaxTime))
}
