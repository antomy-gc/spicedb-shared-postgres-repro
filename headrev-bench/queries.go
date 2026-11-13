package main

const schemaHashForSelectedXID = `
	COALESCE(
		(SELECT hash FROM schema_revision
		 WHERE (selected.xid IS NOT NULL AND created_xid <= selected.xid AND deleted_xid > selected.xid)
		    OR (selected.xid IS NULL AND deleted_xid = '9223372036854775807'::xid8)
		 ORDER BY created_xid DESC LIMIT 1),
		''::bytea
	)`

type variant struct {
	name  string
	match string
	sql   string
}

var variants = []variant{
	{
		name:  "current",
		match: "pg_current_snapshot() as snapshot",
		sql: `
	WITH selected AS (
		SELECT pg_current_xact_id_if_assigned() as xid, pg_current_snapshot() as snapshot
	)
	SELECT selected.snapshot, ` + schemaHashForSelectedXID + `
	FROM selected`,
	},
	{
		name:  "bounded",
		match: "pg_snapshot_xmax(selected.snapshot)",
		sql: `
	WITH selected AS (
		SELECT xid AS xid, snapshot AS snapshot
		FROM relation_tuple_transaction
		ORDER BY xid DESC
		LIMIT 1
	)
	SELECT selected.xid, selected.snapshot,
	ARRAY(
		SELECT rtt.xid FROM relation_tuple_transaction rtt
		WHERE rtt.xid = ANY(ARRAY(SELECT pg_snapshot_xip(selected.snapshot)))
	),
	ARRAY(
		SELECT rtt.xid FROM relation_tuple_transaction rtt
		WHERE rtt.xid >= pg_snapshot_xmax(selected.snapshot) AND rtt.xid < selected.xid
	),
	` + schemaHashForSelectedXID + `
	FROM selected`,
	},
	{
		name:  "range",
		match: "pg_snapshot_xmin(selected.snapshot)",
		sql: `
	WITH selected AS (
		SELECT xid AS xid, snapshot AS snapshot
		FROM relation_tuple_transaction
		ORDER BY xid DESC
		LIMIT 1
	)
	SELECT selected.xid, selected.snapshot,
	ARRAY(
		SELECT rtt.xid FROM relation_tuple_transaction rtt
		WHERE rtt.xid >= pg_snapshot_xmin(selected.snapshot) AND rtt.xid <> selected.xid
	),
	` + schemaHashForSelectedXID + `
	FROM selected`,
	},
}
