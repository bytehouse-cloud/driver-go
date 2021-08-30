package bytehouse

import (
	"context"
	"fmt"
	"reflect"

	"github.com/bytehouse-cloud/driver-go/driver/lib/settings"
)

type QueryContext struct {
	context.Context
	querySettings     map[string]interface{}
	bytehouseSettings map[string]interface{}
	// Bool flag that tells you that the connection has been checked
	hasCheckedConn bool
}

// NewQueryContext initialize a context that can be passed when querying.
//
// Example:
//
// myCtx := bytehouse.NewQueryContext(context.Background())
// myCtx.AddSetting("send_logs_level", "trace")
//
// res, err := db.ExecContext(myCtx, "select 1")
func NewQueryContext(ctx context.Context) *QueryContext {
	if qc, ok := ctx.(*QueryContext); ok {
		return qc
	}
	return &QueryContext{
		Context:           ctx,
		querySettings:     make(map[string]interface{}),
		bytehouseSettings: make(map[string]interface{}),
	}
}

// AddQuerySetting adds a query setting to the query context which will be applied for the query
func (q *QueryContext) AddQuerySetting(name string, value interface{}) error {
	v, err := settings.SettingToValue(name, value)
	if err != nil {
		return err
	}
	q.querySettings[name] = v
	return nil
}

func (q *QueryContext) GetQuerySettings() map[string]interface{} {
	return q.querySettings
}

// AddByteHouseSetting adds a settings which will not be send over to server
func (q *QueryContext) AddByteHouseSetting(name string, value interface{}) error {
	v, err := settingToValue(name, value)
	if err != nil {
		return err
	}
	q.bytehouseSettings[name] = v
	return nil
}

func (q *QueryContext) GetByteHouseSettings() map[string]interface{} {
	return q.bytehouseSettings
}

func (q *QueryContext) GetCheckedConn() bool {
	return q.hasCheckedConn
}

func (q *QueryContext) SetCheckedConn(checkedConn bool) {
	q.hasCheckedConn = checkedConn
}

func settingToValue(name string, value interface{}) (interface{}, error) {
	def, ok := Default[name]
	if !ok {
		return nil, fmt.Errorf("%v is not a bytehouse setting", name)
	}
	if !isType(def, value) {
		return nil, fmt.Errorf("expected type %v should be %T", name, def)
	}
	return value, nil
}

func isType(a, b interface{}) bool {
	return reflect.TypeOf(a) == reflect.TypeOf(b)
}
