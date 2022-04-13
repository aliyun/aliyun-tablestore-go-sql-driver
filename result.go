package tablestore

type noResult struct{}

// LastInsertId returns the database's auto-generated ID after, for example, an INSERT into a table with primary key.
func (noResult) LastInsertId() (int64, error) {
	return 0, nil
}

// RowsAffected returns the number of rows affected by the query.
func (noResult) RowsAffected() (int64, error) {
	return 0, nil
}
