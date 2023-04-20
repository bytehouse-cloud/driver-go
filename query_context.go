package bytehouse

import (
	"context"
	"fmt"
	"reflect"

	"github.com/bytehouse-cloud/driver-go/driver/lib/settings"
)

type QueryContext struct {
	context.Context
	querySettings         map[string]interface{}
	clientSettings        map[string]interface{}
	persistentConnConfigs map[string]interface{}
	temporaryConnConfigs  map[string]interface{}
	queryID               string
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
		Context:               ctx,
		querySettings:         make(map[string]interface{}),
		clientSettings:        make(map[string]interface{}),
		persistentConnConfigs: make(map[string]interface{}),
		temporaryConnConfigs:  make(map[string]interface{}),
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

// AddClientSetting adds a settings which will not be send over to server
func (q *QueryContext) AddClientSetting(name string, value interface{}) error {
	v, err := clientSettingToValue(name, value)
	if err != nil {
		return err
	}
	q.clientSettings[name] = v
	return nil
}

func (q *QueryContext) GetClientSettings() map[string]interface{} {
	return q.clientSettings
}

func (q *QueryContext) AddPersistentConnConfigs(name string, value interface{}) error {
	q.persistentConnConfigs[name] = value
	return nil
}

func (q *QueryContext) GetPersistentConnConfigs() map[string]interface{} {
	return q.persistentConnConfigs
}

func (q *QueryContext) AddTemporaryConnConfigs(name string, value interface{}) error {
	q.temporaryConnConfigs[name] = value
	return nil
}

func (q *QueryContext) GetTemporaryConnConfigs() map[string]interface{} {
	return q.temporaryConnConfigs
}

func (q *QueryContext) GetQueryID() string {
	return q.queryID
}

func (q *QueryContext) SetQueryID(id string) {
	q.queryID = id
}

func clientSettingToValue(name string, value interface{}) (interface{}, error) {
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
