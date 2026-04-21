package store

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/flight"
	genflight "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"github.com/stretchr/testify/assert"
)

func TestMigrationHelper(t *testing.T) {
	mh := NewMigrationHelper()

	// Test FlightDescriptor validation
	err := mh.ValidateFlightDescriptor(nil)
	assert.Error(t, err)
	err = mh.ValidateFlightDescriptor(&flight.FlightDescriptor{})
	assert.NoError(t, err)

	// Test FlightInfo validation
	err = mh.ValidateFlightInfo(nil)
	assert.Error(t, err)
	
	// Missing endpoints
	info := &flight.FlightInfo{}
	err = mh.ValidateFlightInfo(info)
	assert.Error(t, err)
	
	// Valid info
	info.Endpoint = []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: []byte("test")}}}
	err = mh.ValidateFlightInfo(info)
	assert.NoError(t, err)

	// Test Action validation
	err = mh.ValidateAction(nil)
	assert.Error(t, err)
	
	action := &genflight.Action{}
	err = mh.ValidateAction(action)
	assert.Error(t, err)
	
	action.Type = "test"
	err = mh.ValidateAction(action)
	assert.NoError(t, err)
}

func TestFlightCompatibilityLayerBasics(t *testing.T) {
	fcl := NewFlightCompatibilityLayer()
	assert.NotNil(t, fcl)
	
	assert.Equal(t, "v18.5.1 (v23-ready)", fcl.GetAPIVersion())
	assert.False(t, fcl.IsV23Compatible())
	
	err := fcl.PrepareForV23()
	assert.Error(t, err)
}
