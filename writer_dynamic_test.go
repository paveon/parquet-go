package parquet

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"testing"
	"time"
)

type dynamicPort struct {
	Name   string  `parquet:"name"`
	Errors *int64  `parquet:"errors,optional"`
	Util   float64 `parquet:"util"`
}

type dynamicRow struct {
	ID        string            `parquet:"id"`
	At        time.Time         `parquet:"at,timestamp(microsecond)"`
	Count     int64             `parquet:"count"`
	Small     int16             `parquet:"small"`
	Optional  *int64            `parquet:"optional,optional"`
	Model     *string           `parquet:"model,optional"`
	Ratio     float32           `parquet:"ratio"`
	Ok        bool              `parquet:"ok"`
	Blob      []byte            `parquet:"blob"`
	Tags      []string          `parquet:"tags,list"`
	Ports     []dynamicPort     `parquet:"ports,list"`
	Labels    map[string]string `parquet:"labels"`
	Inner     *dynamicPort      `parquet:"inner,optional"`
	Unwritten string            `parquet:"-"`
}

// dynamicRows returns n rows that cover nil, zero and set values of every field.
func dynamicRows(n int) []*dynamicRow {
	rows := make([]*dynamicRow, n)
	for i := range rows {
		v := int64(i)
		zero := int64(0)
		model := fmt.Sprint("m", i%3)
		r := &dynamicRow{
			ID:    fmt.Sprint("id-", i%5),
			At:    time.UnixMicro(int64(i) * 1e6).UTC(),
			Count: v,
			Small: int16(i),
			Ratio: float32(i) / 3,
			Ok:    i%2 == 0,
			Blob:  []byte(fmt.Sprint(i)),
		}
		switch i % 3 {
		case 0:
			r.Optional, r.Model = &v, &model
			r.Tags = []string{"a", "b"}
			r.Ports = []dynamicPort{{Name: "p0", Errors: &v, Util: 0.5}, {Name: "p1", Errors: &zero}}
			r.Labels = map[string]string{"k": "v"}
			r.Inner = &dynamicPort{Name: "inner", Errors: &zero}
		case 1:
			r.Optional = &zero
			r.Tags = []string{}
			r.Ports = []dynamicPort{{Name: "p"}}
		}
		rows[i] = r
	}
	return rows
}

func toAny[T any](rows []T) []any {
	out := make([]any, len(rows))
	for i, r := range rows {
		out[i] = r
	}
	return out
}

// write writes rows in batches of batch with a GenericWriter[T].
func write[T any](t testing.TB, rows []T, batch int, options ...WriterOption) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := NewGenericWriter[T](&buf, append([]WriterOption{PageBufferSize(256)}, options...)...)
	for len(rows) > 0 {
		n := min(batch, len(rows))
		if _, err := w.Write(rows[:n]); err != nil {
			t.Fatal(err)
		}
		rows = rows[n:]
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestGenericWriterAnyWritesAsTypedWriter(t *testing.T) {
	rows := dynamicRows(100)
	tests := []struct {
		name string
		tags []SchemaOption
	}{
		{name: "schema of the type"},
		{
			name: "struct tag options",
			// Writes the tags as a repeated column instead of a list.
			tags: []SchemaOption{StructTag(`parquet:"tags"`, "Tags")},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var options []WriterOption
			for _, tag := range test.tags {
				options = append(options, tag.(WriterOption))
			}
			schema := SchemaOf(new(dynamicRow), test.tags...)
			for _, batch := range []int{1, 7, len(rows)} {
				want := write(t, rows, batch, options...)
				got := write(t, toAny(rows), batch, append([]WriterOption{schema}, options...)...)
				if !bytes.Equal(got, want) {
					t.Errorf("batches of %d: GenericWriter[any] wrote %d bytes, GenericWriter[*T] %d", batch, len(got), len(want))
				}
			}
		})
	}
}

func TestGenericWriterAnyTypesWithoutFastPath(t *testing.T) {
	rows := dynamicRows(20)
	schema := SchemaOf(new(dynamicRow))

	type sameColumns dynamicRow
	values := make([]any, len(rows))
	mixed := make([]any, len(rows))
	for i, r := range rows {
		values[i] = *r
		mixed[i] = r
		if i%2 == 1 {
			mixed[i] = (*sameColumns)(r)
		}
	}

	tests := []struct {
		name string
		rows []any
	}{
		{name: "struct values", rows: values},
		{name: "mixed types", rows: mixed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got, want := write(t, test.rows, 8, schema), write(t, rows, 8); !bytes.Equal(got, want) {
				t.Errorf("wrote %d bytes, want %d", len(got), len(want))
			}
		})
	}
}

func TestWriteRowsFuncOfStructPointer(t *testing.T) {
	schema := SchemaOf(new(dynamicRow))
	retag := StructTagOption{StructTag: `parquet:"tags"`, ColumnPath: []string{"Tags"}}
	retagged := SchemaOf(new(dynamicRow), &retag)
	type otherSchema struct {
		ID string `parquet:"id"`
	}
	tests := []struct {
		name   string
		t      reflect.Type
		schema *Schema
		tags   []StructTagOption
		want   bool
	}{
		{name: "pointer to struct of the schema", t: reflect.TypeFor[*dynamicRow](), schema: schema, want: true},
		{name: "struct tag options of the schema", t: reflect.TypeFor[*dynamicRow](), schema: retagged, tags: []StructTagOption{retag}, want: true},
		{name: "struct tag options not of the schema", t: reflect.TypeFor[*dynamicRow](), schema: schema, tags: []StructTagOption{retag}},
		{name: "struct tag options missing", t: reflect.TypeFor[*dynamicRow](), schema: retagged},
		{name: "struct value", t: reflect.TypeFor[dynamicRow](), schema: schema},
		{name: "pointer to struct of another schema", t: reflect.TypeFor[*otherSchema](), schema: schema},
		{name: "pointer to non-struct", t: reflect.TypeFor[*int64](), schema: schema},
		{name: "nil", t: nil, schema: schema},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := writeRowsFuncOfStructPointer(test.t, test.schema, test.tags) != nil; got != test.want {
				t.Errorf("fast path = %t, want %t", got, test.want)
			}
		})
	}
}

func BenchmarkGenericWriterAny(b *testing.B) {
	rows := dynamicRows(8192)
	anyRows := toAny(rows)
	schema := SchemaOf(new(dynamicRow))

	b.Run("typed", func(b *testing.B) {
		w := NewGenericWriter[*dynamicRow](io.Discard, schema)
		for range b.N {
			if _, err := w.Write(rows); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(rows)), "ns/row")
	})
	b.Run("any", func(b *testing.B) {
		w := NewGenericWriter[any](io.Discard, schema)
		for range b.N {
			if _, err := w.Write(anyRows); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(rows)), "ns/row")
	})
}
