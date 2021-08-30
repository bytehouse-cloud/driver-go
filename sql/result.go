package sql

import (
	"github.com/bytehouse-cloud/driver-go/errors"
)

var emptyResult = &result{}

type result struct{}

func (*result) LastInsertId() (int64, error) {
	return 0, errors.Errorf("LastInsertId is not supported")
}
func (*result) RowsAffected() (int64, error) {
	return 0, errors.Errorf("RowsAffected is not supported")
}
