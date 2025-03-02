// Code generated by mockery v2.9.4. DO NOT EDIT.

package mocks

import (
	context "context"

	mock "github.com/stretchr/testify/mock"
)

// Topic is an autogenerated mock type for the Topic type
type Topic struct {
	mock.Mock
}

// Publish provides a mock function with given fields: ctx, msg, attributes
func (_m *Topic) Publish(ctx context.Context, msg string, attributes ...map[string]interface{}) error {
	_va := make([]interface{}, len(attributes))
	for _i := range attributes {
		_va[_i] = attributes[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, ctx, msg)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, ...map[string]interface{}) error); ok {
		r0 = rf(ctx, msg, attributes...)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// PublishBatch provides a mock function with given fields: ctx, messages, attributes
func (_m *Topic) PublishBatch(ctx context.Context, messages []string, attributes []map[string]interface{}) error {
	ret := _m.Called(ctx, messages, attributes)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, []string, []map[string]interface{}) error); ok {
		r0 = rf(ctx, messages, attributes)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SubscribeSqs provides a mock function with given fields: ctx, queueArn, attributes
func (_m *Topic) SubscribeSqs(ctx context.Context, queueArn string, attributes map[string]interface{}) error {
	ret := _m.Called(ctx, queueArn, attributes)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, map[string]interface{}) error); ok {
		r0 = rf(ctx, queueArn, attributes)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
