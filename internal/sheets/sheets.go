package sheets

import (
	"context"
	"errors"
	"fmt"
	"math/rand"

	log "github.com/sirupsen/logrus"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

type Color = sheets.Color

type Cell struct {
	Text            string
	Hyperlink       string
	BackgroundColor *Color
}

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
	err error

	client  *Client
	table   string
	sheet   string
	sheetId int64
	fields  []string
	values  [][]interface{}
}

func (c *Client) Insert(table string, sheet string) *InsertQuery {
	sheetId, err := c.findSheetId(table, sheet)
	return &InsertQuery{
		client:  c,
		table:   table,
		sheet:   sheet,
		sheetId: sheetId,
		values:  make([][]interface{}, 0),
		err:     err,
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
	if q.err != nil {
		return q.err
	}

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

func formatCellData(value interface{}) *sheets.CellData {
	cell := &sheets.CellData{
		UserEnteredValue:  &sheets.ExtendedValue{},
		UserEnteredFormat: &sheets.CellFormat{},
	}

	if value == nil {
		return cell
	}

	switch v := value.(type) {
	case Cell:
		if v.Hyperlink != "" {
			if v.Text != "" {
				cell.UserEnteredValue.FormulaValue = fmt.Sprintf("=HYPERLINK(\"%s\";\"%s\")", v.Hyperlink, v.Text)
			} else {
				cell.UserEnteredValue.FormulaValue = fmt.Sprintf("=HYPERLINK(\"%s\")", v.Hyperlink)
			}
		} else {
			cell.UserEnteredValue.StringValue = v.Text
		}
		if v.BackgroundColor != nil {
			cell.UserEnteredFormat.BackgroundColor = v.BackgroundColor
		}
	default:
		cell.UserEnteredValue.StringValue = fmt.Sprintf("%s", value)
	}

	return cell
}

func (q *InsertQuery) execute(mapping *columnMapping) error {
	if len(q.fields) == 0 {
		return nil
	}

	rows := make([]*sheets.RowData, len(q.values))

	for i, row := range q.values {
		if len(row) != len(q.fields) {
			return errors.New("Mismatched numbers of values and fields")
		}

		values := make([]*sheets.CellData, mapping.numColumns)

		for j, field := range q.fields {
			values[mapping.columnToIndex[field]] = formatCellData(q.values[i][j])
		}

		rows[i] = &sheets.RowData{
			Values: values,
		}
	}

	err := q.client.batch(q.table, &sheets.Request{
		AppendCells: &sheets.AppendCellsRequest{
			Fields:  "*",
			SheetId: q.sheetId,
			Rows:    rows,
		},
	})

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
	sheetId *int64
	columns []string
}

func (c *Client) Sort(table string, sheet string) *SortQuery {
	sheetId, err := c.findSheetId(table, sheet)
	sheetIdRef := &sheetId
	if err != nil {
		sheetIdRef = nil
	}

	return &SortQuery{
		client:  c,
		table:   table,
		sheet:   sheet,
		sheetId: sheetIdRef,
	}
}

func (q *SortQuery) By(columns ...string) *SortQuery {
	q.columns = columns
	return q
}

func (q *SortQuery) Do() error {
	if q.sheetId == nil {
		return errors.New("Unknown sheet")
	}

	schema, err := loadSchema(q.client, q.table, q.sheet)
	if err != nil {
		return err
	}

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
				SheetId:       *q.sheetId,
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

type Snapshot struct {
	client            *Client
	table             string
	originalSheetName string
	originalSheetId   int64
	tempSheetName     string
	tempSheetId       int64
}

func (c *Client) Snapshot(table string, sheet string) (*Snapshot, error) {
	originalSheetId, err := c.findSheetId(table, sheet)
	if err != nil {
		return nil, err
	}

	snapshot := &Snapshot{
		client:            c,
		table:             table,
		originalSheetName: sheet,
		originalSheetId:   originalSheetId,
		tempSheetId:       int64(rand.Int31()),
		tempSheetName:     randString(16),
	}

	err = snapshot.batch(&sheets.Request{
		DuplicateSheet: &sheets.DuplicateSheetRequest{
			NewSheetId:    snapshot.tempSheetId,
			NewSheetName:  snapshot.tempSheetName,
			SourceSheetId: snapshot.originalSheetId,
		},
	}, &sheets.Request{
		UpdateSheetProperties: &sheets.UpdateSheetPropertiesRequest{
			Fields: "hidden",
			Properties: &sheets.SheetProperties{
				SheetId: snapshot.tempSheetId,
				Hidden:  true,
			},
		},
	})
	if err != nil {
		log.WithError(err).Errorln("Failed to create sheet snapshot")
		return nil, err
	}

	return snapshot, nil
}

func (c *Client) WithSnapshot(table string, sheet string, cb func(*Snapshot) error) error {
	snapshot, err := c.Snapshot(table, sheet)
	if err != nil {
		return err
	}

	err = cb(snapshot)

	if err == nil {
		return snapshot.Commit()
	} else {
		rollbackError := snapshot.Rollback()
		if rollbackError != nil {
			log.WithError(rollbackError).Errorln("Rollback failed")
		}
		return err
	}
}

func (c *Client) findSheetId(table string, sheet string) (int64, error) {
	res, err := c.service.Spreadsheets.Get(table).Fields("sheets").Do()
	if err != nil {
		return 0, err
	}

	for _, sheetRef := range res.Sheets {
		if sheet == sheetRef.Properties.Title {
			return sheetRef.Properties.SheetId, nil
		}
	}
	return 0, errors.New("Unknown sheet")
}

func (s *Snapshot) Insert() *InsertQuery {
	return s.client.Insert(s.table, s.tempSheetName)
}

func (s *Snapshot) Delete() *DeleteQuery {
	return s.client.Delete(s.table, s.tempSheetName)
}

func (s *Snapshot) Sort() *SortQuery {
	return s.client.Sort(s.table, s.tempSheetName)
}

func (s *Snapshot) Commit() error {
	return s.batch(&sheets.Request{
		DeleteRange: &sheets.DeleteRangeRequest{
			Range: &sheets.GridRange{
				SheetId: s.originalSheetId,
			},
			ShiftDimension: "ROWS",
		},
	}, &sheets.Request{
		CopyPaste: &sheets.CopyPasteRequest{
			Source: &sheets.GridRange{
				SheetId: s.tempSheetId,
			},
			Destination: &sheets.GridRange{
				SheetId: s.originalSheetId,
			},
		},
	}, &sheets.Request{
		DeleteSheet: &sheets.DeleteSheetRequest{
			SheetId: s.tempSheetId,
		},
	})
}

func (s *Snapshot) Rollback() error {
	return s.batch(&sheets.Request{
		DeleteSheet: &sheets.DeleteSheetRequest{
			SheetId: s.tempSheetId,
		},
	})
}

func (c *Client) batch(table string, requests ...*sheets.Request) error {
	req := &sheets.BatchUpdateSpreadsheetRequest{
		Requests: requests,
	}

	res, err := c.service.Spreadsheets.BatchUpdate(table, req).Do()
	_ = res

	if err != nil {
		return err
	}

	return nil
}

func (s *Snapshot) batch(requests ...*sheets.Request) error {
	return s.client.batch(s.table, requests...)
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}
