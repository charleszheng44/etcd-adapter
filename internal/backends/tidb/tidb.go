package tidb

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/api7/gopkg/pkg/log"
	_ "github.com/go-sql-driver/mysql"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/pkg/errors"
)

type Options struct {
	Host     string
	Port     string
	Database string
	Log      *log.Logger
}

func (tc *tidbCache) createTable() error {
	// TODO (chao.zheng): should we include `version` and `create_revision`?
	// Etcd data model: https://etcd.io/docs/v3.4/learning/data_model/
	// revision v.s. mod revision v.s. version:
	// https://github.com/etcd-io/etcd/issues/6518
	// old_v is used for the update events
	_, err := tc.db.Exec(`
		CREATE TABLE IF NOT EXISTS _bigetc_store (
			k VARCHAR(255) NOT NULL,
			v MEDIUMBLOB NOT NULL,
			crt BOOLEAN,
			del BOOLEAN,
			revision BIGINT NOT NULL,
			prev_rev BIGINT,
			lease INTEGER,
			PRIMARY KEY (k, revision) CLUSTERED
		);
	`)
	if err != nil {
		return err
	}
	// TODO (chao.zheng): remove the fk, as TiDB does not support
	// constraint checking on fk
	_, err = tc.db.Exec(`
		CREATE TABLE IF NOT EXISTS _bigetc_curr_rev (
			k VARCHAR(255) PRIMARY KEY CLUSTERED NOT NULL,
			revision BIGINT NOT NULL,
			FOREIGN KEY 
				fk_krev(k, revision) 
			REFERENCES 
				_bigetc_store(k, revision)
		);
	`)
	return err
}

func NewTiDBCache(ctx context.Context, opt *Options, logger *log.Logger) (server.Backend, error) {
	db, err := sql.Open("mysql",
		fmt.Sprintf("root:@tcp(%s:%s)/%s", opt.Host, opt.Port, opt.Database))
	if err != nil {
		return nil, err
	}

	tc := &tidbCache{
		logger: logger,
		db:     db,
	}

	err = tc.createTable()
	if err != nil {
		return nil, err
	}

	return tc, nil
}

type tidbCache struct {
	logger *log.Logger
	db     *sql.DB
}

var _ = server.Backend(&tidbCache{})

func (tc *tidbCache) Start(ctx context.Context) error {
	tc.logger.Warn("key compaction function has not implemented yet")
	return nil
}

func (tc *tidbCache) Get(ctx context.Context,
	key string, revision int64) (int64, *server.KeyValue, error) {
	currRev, err := getTSO(tc.db)
	if err != nil {
		return 0, nil, err
	}
	if err := parseRevision(tc.db, key, &revision); err != nil {
		return currRev, nil, nil
	}

	kv, err := queryKV(tc.db.QueryRow, key, revision)
	if err != nil {
		return currRev, nil, err
	}
	return currRev, kv, nil
}

func (tc *tidbCache) Create(ctx context.Context,
	key string, value []byte, lease int64) (currRev int64, err error) {
	currRev, err = getTSO(tc.db)
	if err != nil {
		return
	}

	txn, err := tc.db.Begin()
	if err != nil {
		return
	}
	defer txn.Rollback()

	_, err = txn.Exec(`
		SELECT 
			k 
		FROM
			_bigetc_store
		WHERE k = ?
		FOR UPDATE
	`, key)
	if err != nil {
		return
	}
	_, err = txn.Exec(`
		INSERT INTO 
			_bigetc_store (k, v, crt, del, revision, lease)
		VALUES 
			(?, ?, ?, ?, ?, ?)
	`, key, value, true, false, currRev, lease)
	if err != nil {
		return
	}

	_, err = txn.Exec(`
		SELECT 
			k 
		FROM
			_bigetc_curr_rev
		WHERE k = ?
		FOR UPDATE
	`, key)
	if err != nil {
		return
	}
	_, err = txn.Exec(`
		INSERT INTO 
			_bigetc_curr_rev (k, revision)
		VALUES 
			(?, ?) 
	`, key, currRev)
	if err != nil {
		return
	}

	err = txn.Commit()
	return
}

func (tc *tidbCache) Delete(ctx context.Context,
	key string, revision int64) (int64, *server.KeyValue, bool, error) {
	currRev, err := getTSO(tc.db)
	if err != nil {
		return 0, nil, false, err
	}
	txn, err := tc.db.Begin()
	if err != nil {
		return currRev, nil, false, err
	}
	defer txn.Rollback()

	exist, latestRev, err := getLatestRevision(tc.db, key)
	if !exist {
		return currRev, nil, false, err
	}

	if revision != 0 && latestRev > revision {
		// Delete will only delete the entry with the latest revision.
		return currRev, nil, false, err
	}
	revision = latestRev

	// delete the latest revision
	if _, err := txn.Exec(`
		DELETE FROM
			_bigetc_curr_rev
		WHERE k = ?
	`, key); err != nil {
		return currRev, nil, false, err
	}
	// Delete the entry of the revision
	// 1. get the target kv
	// TODO (chao.zheng) is this necessary? just return `nil` in success?
	kv, err := queryKV(txn.QueryRow, key, revision)
	if err != nil {
		return currRev, nil, false, err
	}
	// 2. delete the target kv
	// NOTE since watcher need to detect the delete event, mark the entry
	// as deleted
	if _, err := txn.Exec(`
		SELECT
			k 
		FROM
			_bigetc_store
		WHERE
			k = ?
		AND
			revision = ?
		FOR UPDATE
	`, key, revision); err != nil {
		return currRev, nil, false, err
	}

	if _, err := txn.Exec(`
		INSERT INTO	
			_bigetc_store (k, v, revision, del)
		VALUES
			(?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			del = VALUES(del)
	`, key, kv.Value, revision, true); err != nil {
		return currRev, nil, false, err
	}

	if err := txn.Commit(); err != nil {
		return currRev, nil, false, err
	}
	return currRev, kv, true, nil
}

// TODO (chao.zheng) the `startKey` is useless?
func (tc *tidbCache) List(ctx context.Context,
	prefix, startKey string,
	limit, revision int64) (int64, []*server.KeyValue, error) {
	currRev, err := getTSO(tc.db)
	if err != nil {
		return 0, nil, err
	}
	if revision == 0 {
		// list the latest kvs whose k contains the `prefix`
		query := fmt.Sprintf(`
			SELECT
				k, v, crt, revision, lease
			FROM 
				_bigetc_store
			WHERE
				(k, revision)
			IN
				(SELECT
					k, revision
				 FROM 
				 	_bigetc_curr_rev
				 WHERE
				    k
				 LIKE
				 	'%s')`, prefix+"%")
		rows, err := tc.db.Query(query)
		if err != nil {
			return currRev, nil, err
		}

		kvs, err := rowsToKVs(rows)
		if err != nil {
			return currRev, nil, err
		}
		return currRev, kvs, nil
	}
	// list kvs whose k contains the `prefix` with specified revision
	rows, err := tc.db.Query(`
		SELECT	
			k, v, crt, revision, lease
		FROM
			_bigetc_store
		WHERE
			k 
		IN
			'?'
		AND
			revision = ?
	`, prefix+"%", revision)
	if err != nil {
		return currRev, nil, err
	}
	kvs, err := rowsToKVs(rows)
	if err != nil {
		return currRev, nil, err
	}
	return currRev, kvs, nil
}

func (tc *tidbCache) Count(ctx context.Context,
	prefix string) (currRev int64, count int64, err error) {
	currRev, err = getTSO(tc.db)
	if err != nil {
		return
	}
	err = tc.db.QueryRow(`
		SELECT	
			count(k)
		FROM
			_bigetc_store
		WHERE
			k 
		LIKE
			'?'
	`, prefix+"%").Scan(&count)
	if err != nil {
		return
	}
	return
}

func (tc *tidbCache) Update(ctx context.Context,
	key string, value []byte,
	revision, lease int64) (
	int64, *server.KeyValue, bool, error) {
	currRev, err := getTSO(tc.db)
	if err != nil {
		return 0, nil, false, err
	}

	txn, err := tc.db.Begin()
	if err != nil {
		return currRev, nil, false, err
	}
	defer txn.Rollback()

	exist, latestRev, err := getLatestRevision(tc.db, key)
	if exist && revision < latestRev {
		// the Update can only be applied on the latest revision
		return currRev, nil, false,
			errors.Errorf(
				"given revision(%d)"+
					"is smaller than"+
					" the latest revision(%d)", revision)
	}

	// Updating the latest revision
	// 1. update the concurrent revision for the `key`
	if _, err := txn.Exec(`
		SELECT
			k 
		FROM
			_bigetc_curr_rev
		WHERE
			k = ?
		FOR UPDATE
	`, key); err != nil {
		return currRev, nil, false, err
	}
	if _, err := txn.Exec(`
		INSERT INTO
			_bigetc_curr_rev (k, revision)
		VALUES 
			(?, ?)
		ON DUPLICATE KEY UPDATE
			revision = VALUES(revision)
	`, key, currRev); err != nil {
		return currRev, nil, false, err
	}
	// 2. insert the new entry into the _bigetc_store
	if _, err = txn.Exec(`
		INSERT INTO 
			_bigetc_store (k, v, crt, del, revision, prev_rev, lease)
		VALUES 
			(?, ?, ?, ?, ?, ?, ?) 
	`, key, value, false, false, currRev, latestRev, lease); err != nil {
		return currRev, nil, false, err
	}

	err = txn.Commit()
	if err != nil {
		return currRev, nil, false, err
	}

	kv := &server.KeyValue{
		Key:         key,
		ModRevision: currRev,
		Value:       value,
		Lease:       lease,
	}
	return currRev, kv, true, err
}

func (tc *tidbCache) Watch(ctx context.Context,
	prefix string, revision int64) <-chan []*server.Event {
	watchChan := make(chan []*server.Event)

	// tmpRev is used to mark the largest revision that has being sent
	tmpRev := revision
	go func() {
		defer func() {
			close(watchChan)
		}()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				// get all events with revision larger than the lastRevision
				// more recent events will be at the further back of the slice
				rows, err := tc.db.Query(`
					SELECT
						k, v, crt, del, revision, prev_rev, lease
					FROM
						_bigetc_store
					WHERE
						k
					LIKE
						'?'
					AND
						revision > ?
					ORDER BY
						revision
					ASC
				`, prefix+"%", tmpRev)
				if err != nil {
					return
				}

				// push all the qualified events
				events, err := rowsToEvents(tc.db.QueryRow, rows)
				if err != nil {
					log.Errorf("failed to convert rows to events: %v", err)
					close(watchChan)
				}
				watchChan <- events

				// update the tmpRev
				tmpRev = events[len(events)-1].KV.ModRevision
			}
		}
	}()

	return watchChan
}

func (tc *tidbCache) DbSize(ctx context.Context) (int64, error) {
	var size int64
	return size, tc.db.QueryRow(`
		SELECT 
			SUM(data_length + index_length) 
		FROM 
			information_schema.TABLES 
		WHERE 
			table_schema = DATABASE() 
		AND 
			table_name 
		IN 
			('_bigetc_store', '_bigetc_curr_rev')
	`).Scan(&size)
}
