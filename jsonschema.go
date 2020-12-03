package main

import (
	"fmt"

	"github.com/nats-io/jsm.go/api"
	"github.com/xeipuuv/gojsonschema"
)

type SchemaValidator struct{}

func (v SchemaValidator) ValidateStruct(data interface{}, schemaType string) (ok bool, errs []string) {
	s, err := api.Schema(schemaType)
	if err != nil {
		return false, []string{"unknown schema type %s", schemaType}
	}

	ls := gojsonschema.NewBytesLoader(s)
	ld := gojsonschema.NewGoLoader(data)
	result, err := gojsonschema.Validate(ls, ld)
	if err != nil {
		return false, []string{fmt.Sprintf("validation failed: %s", err)}
	}

	if result.Valid() {
		return true, nil
	}

	errors := make([]string, len(result.Errors()))
	for i, verr := range result.Errors() {
		errors[i] = verr.String()
	}

	return false, errors
}
