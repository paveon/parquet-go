package parquet

import (
	"reflect"
	"unsafe"

	"github.com/parquet-go/parquet-go/sparse"
)

// writeRowsFuncOfDynamicType returns the writeRowsFunc of a GenericWriter[T]
// whose T is an interface type, such as any, so that the Go type of its rows is
// only known once they are written.
//
// Rows that are all pointers to structs of one type, whose schema is the
// writer's, are written as a GenericWriter of that pointer type writes them:
// with functions generated for the type, which copy each field to its column
// from its offset in the struct. Any other rows are written by fallback, which
// looks up every field of every row by name through reflection, and so is
// several times slower.
//
// The function generated for a type is kept until rows of another type are
// written. Like the GenericWriter, it must not be called concurrently.
func writeRowsFuncOfDynamicType[T any](schema *Schema, tagReplacements []StructTagOption, fallback writeRowsFunc) writeRowsFunc {
	var (
		rowType   reflect.Type
		writeRows writeRowsFunc // nil if rows of rowType are written by fallback
		pointers  []unsafe.Pointer
	)
	return func(columns []ColumnBuffer, levels columnLevels, rows sparse.Array) {
		if rows.Len() == 0 {
			fallback(columns, levels, rows)
			return
		}
		t := reflect.TypeOf(any(*(*T)(rows.Index(0))))
		if t != rowType {
			rowType, writeRows = t, writeRowsFuncOfStructPointer(t, schema, tagReplacements)
		}
		if writeRows == nil {
			fallback(columns, levels, rows)
			return
		}

		pointers = pointers[:0]
		for i := range rows.Len() {
			v := reflect.ValueOf(any(*(*T)(rows.Index(i))))
			if v.Type() != rowType {
				// Rows of mixed types.
				clear(pointers)
				fallback(columns, levels, rows)
				return
			}
			pointers = append(pointers, v.UnsafePointer())
		}
		writeRows(columns, levels, makeArrayFromSlice(pointers))
		// Do not keep the rows alive.
		clear(pointers)
	}
}

// writeRowsFuncOfStructPointer returns the writeRowsFunc of values of t if t is
// a pointer to a struct whose schema is schema, or nil otherwise.
func writeRowsFuncOfStructPointer(t reflect.Type, schema *Schema, tagReplacements []StructTagOption) writeRowsFunc {
	if t == nil || t.Kind() != reflect.Pointer || t.Elem().Kind() != reflect.Struct {
		return nil
	}
	if _, ok := validateColumns(t.Elem()); !ok {
		return nil
	}
	if !EqualNodes(schema, schemaOf(t.Elem(), tagReplacements...)) {
		return nil
	}
	return writeRowsFuncOf(t, schema, nil, tagReplacements)
}
