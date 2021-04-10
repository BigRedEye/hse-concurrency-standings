package sheets

import (
	"context"
	"errors"
	"fmt"

	log "github.com/sirupsen/logrus"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

type Client struct {
	service *sheets.Service
}

func NewClient(ctx context.Context, credentialsFile string) (*Client, error) {
	service, err := sheets.NewService(ctx, option.WithCredentialsFile(credentialsFile))
	if err != nil {
		return nil, err
	}

	return &Client{
		service: service,
	}, nil
}

type InsertQuery struct {
	client *Client
	table  string
	sheet  string
	fields []string
	values [][]interface{}
}

func (c *Client) Insert(table string, sheet string) *InsertQuery {
	return &InsertQuery{
		client: c,
		table:  table,
		sheet:  sheet,
		values: make([][]interface{}, 0),
	}
}

func (q *InsertQuery) Into(fields ...string) *InsertQuery {
	q.fields = fields
	return q
}

func (q *InsertQuery) Values(values ...interface{}) *InsertQuery {
	q.values = append(q.values, values)
	return q
}

func (q *InsertQuery) Rows(rows [][]interface{}) *InsertQuery {
	q.values = append(q.values, rows...)
	return q
}

func (q *InsertQuery) Do() error {
	if len(q.values) == 0 {
		return nil
	}

	mapping, err := q.getSchema()
	if err != nil {
		return err
	}

	if err := q.execute(mapping); err != nil {
		return err
	}

	return nil
}

type columnMapping struct {
	numColumns    int
	columnToIndex map[string]int
}

func newMapping() *columnMapping {
	return &columnMapping{
		numColumns:    0,
		columnToIndex: make(map[string]int),
	}
}

func newMappingFromValueRange(vrange *sheets.ValueRange) (*columnMapping, error) {
	if len(vrange.Values) != 1 {
		return nil, errors.New("Expected exactly one row")
	}

	mapping := newMapping()
	for i, field := range vrange.Values[0] {
		name, ok := field.(string)
		if !ok {
			return nil, errors.New("Values should be strings")
		}

		mapping.columnToIndex[name] = i
		mapping.numColumns++
	}

	return mapping, nil
}

func newMappingFromFields(fields ...string) *columnMapping {
	mapping := newMapping()
	for i, field := range fields {
		mapping.columnToIndex[field] = i
		mapping.numColumns++
	}
	return mapping
}

func (m *columnMapping) add(field string) int {
	if index, found := m.columnToIndex[field]; found {
		return index
	}
	index := m.numColumns
	m.numColumns++
	m.columnToIndex[field] = index
	return index
}

func (q *InsertQuery) getSchema() (*columnMapping, error) {
	mapping, err := loadSchema(q.client, q.table, q.sheet)
	if err != nil {
		return nil, err
	}

	if mapping == nil {
		mapping, err = q.setSchema()
		if err != nil {
			return nil, err
		}

		if mapping == nil || mapping.numColumns != len(q.fields) {
			return nil, errors.New("Failed to map columns")
		}
	} else {
		mapping, err = q.validateSchema(mapping)
		if err != nil {
			return nil, err
		}
	}

	return mapping, nil
}

func loadSchema(client *Client, table string, sheet string) (*columnMapping, error) {
	firstRowRange := sheet + "!1:1"

	res, err := client.service.Spreadsheets.Values.Get(table, firstRowRange).Do()
	if err != nil {
		log.WithError(err).Errorln("Failed to get first table row")
		return nil, err
	}

	if len(res.Values) == 0 || len(res.Values[0]) == 0 {
		return nil, nil
	}

	if len(res.Values) != 1 {
		log.Errorf("Failed to get first table row")
		return nil, errors.New(fmt.Sprintf("Expected row of length 1, not %d (%v)", len(res.Values), res.Values))
	}

	return newMappingFromValueRange(res)
}

func (q *InsertQuery) setSchema() (*columnMapping, error) {
	mapping := newMappingFromFields(q.fields...)
	if err := q.putSchema(mapping); err != nil {
		return nil, err
	}
	return mapping, nil
}

func (q *InsertQuery) putSchema(mapping *columnMapping) error {
	valueRange := &sheets.ValueRange{
		Values: make([][]interface{}, 1),
	}
	valueRange.Values[0] = make([]interface{}, mapping.numColumns)

	for field, index := range mapping.columnToIndex {
		valueRange.Values[0][index] = field
	}

	_, err := q.client.service.Spreadsheets.Values.Update(q.table, q.sheet, valueRange).ValueInputOption("RAW").Do()
	if err != nil {
		log.WithError(err).Errorln("Failed to put table schema")
		return err
	}

	return nil
}

func (q *InsertQuery) validateSchema(mapping *columnMapping) (*columnMapping, error) {
	hasUnknownField := false
	for _, field := range q.fields {
		_, found := mapping.columnToIndex[field]
		if !found {
			hasUnknownField = true
			mapping.add(field)
		}
	}

	if hasUnknownField {
		if err := q.putSchema(mapping); err != nil {
			return nil, err
		}
	}

	return mapping, nil
}

func (q *InsertQuery) execute(mapping *columnMapping) error {
	if len(q.fields) == 0 {
		return nil
	}

	valueRange := &sheets.ValueRange{
		Values: make([][]interface{}, len(q.values)),
	}

	for i, row := range q.values {
		if len(row) != len(q.fields) {
			return errors.New("Mismatched numbers of values and fields")
		}
		valueRange.Values[i] = make([]interface{}, mapping.numColumns)

		for j, field := range q.fields {
			valueRange.Values[i][mapping.columnToIndex[field]] = q.values[i][j]
		}
	}

	_, err := q.client.service.Spreadsheets.Values.Append(q.table, q.sheet, valueRange).ValueInputOption("RAW").Do()
	if err != nil {
		log.WithError(err).Errorln("Failed to append values")
		return err
	}

	return nil
}

type DeleteQuery struct {
	client *Client
	table  string
	sheet  string
}

func (c *Client) Delete(table string, sheet string) *DeleteQuery {
	return &DeleteQuery{
		client: c,
		table:  table,
		sheet:  sheet,
	}
}

func (q *DeleteQuery) Do() error {
	_, err := q.client.service.Spreadsheets.Values.Clear(q.table, q.sheet, &sheets.ClearValuesRequest{}).Do()
	return err
}

type SortQuery struct {
	client  *Client
	table   string
	sheet   string
	columns []string
}

func (c *Client) Sort(table string, sheet string) *SortQuery {
	return &SortQuery{
		client: c,
		table:  table,
		sheet:  sheet,
	}
}

func (q *SortQuery) By(columns ...string) *SortQuery {
	q.columns = columns
	return q
}

func (q *SortQuery) Do() error {
	schema, err := loadSchema(q.client, q.table, q.sheet)

	specs := make([]*sheets.SortSpec, len(q.columns))
	for i := range specs {
		specs[i] = &sheets.SortSpec{
			SortOrder:      "ASCENDING",
			DimensionIndex: int64(schema.columnToIndex[q.columns[i]]),
		}
	}

	requests := []*sheets.Request{{
		SortRange: &sheets.SortRangeRequest{
			Range: &sheets.GridRange{
				SheetId:       0,
				StartRowIndex: 1,
			},
			SortSpecs: specs,
		},
	}}

	req := &sheets.BatchUpdateSpreadsheetRequest{
		Requests: requests,
	}

	res, err := q.client.service.Spreadsheets.BatchUpdate(q.table, req).Do()
	_ = res

	if err != nil {
		return err
	}

	return nil
}
