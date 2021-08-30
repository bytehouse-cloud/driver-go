package column

import (
	"reflect"

	"github.com/bytehouse-cloud/driver-go/driver/lib/ch_encoding"
)

type CHColumn struct {
	Name string
	Type CHColumnType
	Data CHColumnData

	GenerateColumn func(numRows int) CHColumnData // Acts like cache to fast generate complex data types of clickhouse
}

func ReadColumnFromDecoder(decoder *ch_encoding.Decoder, numRows int) (*CHColumn, error) {
	var (
		c   CHColumn
		err error
	)
	c.Name, err = decoder.String()
	if err != nil {
		return nil, err
	}
	s, err := decoder.String()
	if err != nil {
		return nil, err
	}
	c.Type = CHColumnType(s)
	c.GenerateColumn, err = GenerateColumnDataFactory(c.Type)
	if err != nil {
		return nil, err
	}
	c.Data = c.GenerateColumn(numRows)
	if err = c.Data.ReadFromDecoder(decoder); err != nil {
		return nil, err
	}
	return &c, nil
}

func WriteColumnToEncoder(encoder *ch_encoding.Encoder, c *CHColumn) error {
	if err := encoder.String(c.Name); err != nil {
		return err
	}
	if err := encoder.String(string(c.Type)); err != nil {
		return err
	}
	return c.Data.WriteToEncoder(encoder)
}

// StructureCopy copies the CHColumn into newCHColumn with n rows
func (c *CHColumn) StructureCopy(n int) *CHColumn {
	return &CHColumn{
		Name:           c.Name,
		Type:           c.Type,
		GenerateColumn: c.GenerateColumn,
		Data:           c.GenerateColumn(n),
	}
}

func (c *CHColumn) Close() error {
	return c.Data.Close()
}

func (c *CHColumn) GetAllRowsFmt(result []string) []string {
	dataLen := c.Data.Len()
	for i := 0; i < dataLen; i++ {
		result = append(result, c.Data.GetString(i))
	}
	return result
}

func (c *CHColumn) GetAllRows(result []interface{}) []interface{} {
	dataLen := c.Data.Len()
	for i := 0; i < dataLen; i++ {
		result = append(result, c.Data.GetValue(i))
	}
	return result
}

func (c *CHColumn) CHType() string {
	return string(c.Type)
}

func (c *CHColumn) ScanType() reflect.Type {
	return reflect.ValueOf(c.Data.Zero()).Type()
}
