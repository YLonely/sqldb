package sqldb

import (
	"errors"
	"reflect"
)

type fieldIterator func(fieldAddr reflect.Value, path []reflect.StructField) (bool, error)

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
		dive, err := fi(fieldAddr, p)
		if err != nil {
			return err
		}
		if dive {
			if err := iterateFields(fieldAddr.Interface(), fi, p...); err != nil {
				return err
			}
		}
	}
	return nil
}

func MapErr[T any, R any](collection []T, iteratee func(T, int) (R, error)) ([]R, error) {
	result := make([]R, len(collection))

	for i, item := range collection {
		res, err := iteratee(item, i)
		if err != nil {
			return nil, err
		}
		result[i] = res
	}

	return result, nil
}
