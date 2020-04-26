// Code generated by go-swagger; DO NOT EDIT.

package types

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/swag"
)

// HeartBeatResponse heart beat response
// swagger:model HeartBeatResponse
type HeartBeatResponse struct {

	// The array of seed taskID which now are selected as seed for the peer. If peer have other seed file which
	// is not included in the array, these seed file should be weed out.
	//
	SeedTaskIds []string `json:"seedTaskIDs"`

	// The version of supernode. If supernode restarts, version should be different, so dfdaemon could know
	// the restart of supernode.
	//
	Version string `json:"version,omitempty"`
}

// Validate validates this heart beat response
func (m *HeartBeatResponse) Validate(formats strfmt.Registry) error {
	return nil
}

// MarshalBinary interface implementation
func (m *HeartBeatResponse) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *HeartBeatResponse) UnmarshalBinary(b []byte) error {
	var res HeartBeatResponse
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
