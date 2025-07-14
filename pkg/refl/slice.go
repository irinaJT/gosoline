package refl

import (
	"fmt"
	"reflect"
)

func InterfaceToInterfaceSlice(in any) ([]any, error) {
	if si, ok := in.([]any); ok {
		return si, nil
	}

	val := reflect.ValueOf(in)

	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Slice {
		return nil, fmt.Errorf("input is not an slice but instead of type %T", in)
	}

	out := make([]any, val.Len())

	for i := 0; i < val.Len(); i++ {
		out[i] = val.Index(i).Interface()
	}

	return out, nil
}

type sliceIterator struct {
	current int
	length  int
	slice   reflect.Value
}

func (s *sliceIterator) Next() bool {
	return s.current < s.length
}

func (s *sliceIterator) Len() int {
	return s.length
}

func (s *sliceIterator) Val() any {
	c := s.current
	s.current++

	return s.slice.Index(c).Interface()
}

func SliceInterfaceIterator(slice any) *sliceIterator {
	_, sv := ResolveValueTo(slice, reflect.Slice)

	return &sliceIterator{
		current: 0,
		length:  sv.Len(),
		slice:   sv,
	}
}

func SliceOf(slice any) (*Slice, error) {
	sliceType := reflect.TypeOf(slice)

	if sliceType.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("the slice has to be addressable")
	}

	sliceType = sliceType.Elem()
	sliceValue := reflect.ValueOf(slice)

	if sliceValue.Kind() == reflect.Ptr {
		sliceValue = sliceValue.Elem()
	}

	elementType := sliceType.Elem()
	elementPtr := false

	if elementType.Kind() == reflect.Ptr {
		elementType = elementType.Elem()
		elementPtr = true
	}

	sr := &Slice{
		slice:       slice,
		sliceType:   sliceType,
		sliceValue:  sliceValue,
		elementType: elementType,
		elementPtr:  elementPtr,
	}

	return sr, nil
}

type Slice struct {
	slice       any
	sliceType   reflect.Type
	sliceValue  reflect.Value
	elementType reflect.Type
	elementPtr  bool
}

func (s *Slice) NewElement() any {
	return reflect.New(s.elementType).Interface()
}

func (s *Slice) Append(elem any) error {
	ev := reflect.ValueOf(elem)

	if s.elementPtr && ev.Kind() != reflect.Ptr {
		return fmt.Errorf("the value which you try to append to the slice has to be addressable")
	}

	if !s.elementPtr && ev.Kind() == reflect.Ptr {
		ev = reflect.Indirect(ev)
	}

	s.sliceValue.Set(reflect.Append(s.sliceValue, ev))

	return nil
}
