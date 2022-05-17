package tidb

import (
	"database/sql"
	"strconv"

	"github.com/api7/gopkg/pkg/log"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/pkg/errors"
)

// TODO (chao.zheng): will this become the performance bottleneck?
func getTSO(db *sql.DB) (int64, error) {
	rows, err := db.Query("show master status")
	if err != nil {
		return 0, err
	}
	values := make([]interface{}, 5)
	for i := 0; i < 5; i++ {
		values[i] = new(string)
	}
	for rows.Next() {
		err = rows.Scan(values...)
		if err != nil {
			log.Fatal(err)
		}
	}
	sbs := *(values[1].(*string))
	return strconv.ParseInt(sbs, 10, 64)
}

func getLatestRevision(db *sql.DB, key string) (bool, int64, error) {
	var revision int64
	if err := db.QueryRow(`
		Select 
			revision
		FROM 
			_bigetc_curr_rev
		WHERE
			k = ?
	`, key).Scan(&revision); err != nil {
		return false, revision, err
	}

	return true, revision, nil
}

// NOTE SIDE EFFECTS this function may change the value of `revision`.
func parseRevision(db *sql.DB, key string, revision *int64) error {
	if *revision != 0 {
		return nil
	}
	exist, rev, err := getLatestRevision(db, key)
	if !exist || err != nil {
		return err
	}
	*revision = rev
	return nil
}

// rowsToKVs converts sql rows to KeyValue slice.
func rowsToKVs(rows *sql.Rows) ([]*server.KeyValue, error) {
	kvs := []*server.KeyValue{}
	for rows.Next() {
		var (
			kv     server.KeyValue
			create bool
		)
		if err := rows.Scan(&kv.Key, &kv.Value,
			&create, &kv.ModRevision, &kv.Lease); err != nil {
			return nil, err
		}
		if create {
			kv.CreateRevision = kv.ModRevision
		}
		kvs = append(kvs, &kv)
	}
	return kvs, nil
}

type queryRowFn func(query string, args ...interface{}) *sql.Row

func queryKV(qf queryRowFn,
	key string, revision int64) (*server.KeyValue, error) {
	kv := &server.KeyValue{}
	var create bool
	if err := qf(`
		SELECT 
			v, crt, lease
		FROM 
			_bigetc_store 
		WHERE
			k = ?
		AND
			revision = ?	
		LIMIT 1
	`, key, revision).Scan(&kv.Value, &create, &kv.Lease); err != nil {
		if err == sql.ErrNoRows {
			// target entry not found
			return nil, nil
		}
		return nil, err
	}
	kv.Key = key
	if create {
		kv.CreateRevision = revision
	}
	return kv, nil
}

func rowsToEvents(qf queryRowFn, rows *sql.Rows) ([]*server.Event, error) {
	events := []*server.Event{}
	for rows.Next() {
		var (
			kv      server.KeyValue
			create  bool
			del     bool
			prevRev int64
		)
		if err := rows.Scan(&kv.Key, &kv.Value,
			&create, &del,
			&kv.ModRevision, &prevRev, &kv.Lease); err != nil {
			return nil, err
		}
		if create {
			kv.CreateRevision = kv.ModRevision
		}

		event := &server.Event{
			Delete: del,
			Create: create,
			KV:     &kv,
		}

		if !create && !del {
			// the event is an UPDATE event, if it is not a CREATE
			// nor a DELETE event and we need to get the previous KV.
			if qf == nil {
				return events, errors.New("the queryRowFn is not set")
			}
			prevKV, err := queryKV(qf, kv.Key, prevRev)
			if err != nil {
				return events, errors.Wrap(err, "failed to queryKV")
			}
			event.PrevKV = prevKV
		}

		events = append(events, event)
	}
	return events, nil
}
