package sqldb

import (
	"errors"
	"reflect"
)

type fieldIterator func(fieldAddr reflect.Value, path []reflect.StructField) bool

func iterateFields(obj any, fi fieldIterator, path ...reflect.StructField) error {
	rv := reflect.ValueOf(obj)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return errors.New("object provided is not a ptr or it's nil")
	}
	e := rv.Elem()
	if e.Kind() != reflect.Struct {
		return nil
	}
	for i := 0; i < e.NumField(); i++ {
		typeField := e.Type().Field(i)
		if !typeField.IsExported() {
			continue
		}
		fieldAddr := e.Field(i).Addr()
		p := append(path, typeField)
		if fi(fieldAddr, p) {
			if err := iterateFields(fieldAddr.Interface(), fi, p...); err != nil {
				return err
			}
		}
	}
	return nil
}
