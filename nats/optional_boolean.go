package main

import (
	"fmt"
	"strconv"

	"gopkg.in/alecthomas/kingpin.v2"
)

// OptionalBoolValue is contains a *bool and implements kingpin.Value
type OptionalBoolValue struct {
	val *bool
}

func OptionalBoolean(s kingpin.Settings) *OptionalBoolValue {
	target := &OptionalBoolValue{}
	s.SetValue(target)
	return target
}

func (v *OptionalBoolValue) SetBool(val bool) {
	v.val = &val
}

func (v *OptionalBoolValue) IsSetByUser() bool { return v.val != nil }

func (v *OptionalBoolValue) Value() bool {
	if v.val == nil {
		return false
	}
	return *v.val
}

func (v *OptionalBoolValue) Set(s string) error {
	b, err := strconv.ParseBool(s)
	if err != nil {
		return err
	}
	v.val = &b

	return nil
}

func (v *OptionalBoolValue) String() string {
	if v.val == nil {
		return ""
	}

	return fmt.Sprintf("%v", *v.val)
}

func (v *OptionalBoolValue) IsBoolFlag() bool { return true }
