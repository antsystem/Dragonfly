// Code generated by go-swagger; DO NOT EDIT.

package types

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/swag"
)

// Extra extra
// swagger:model Extra
type Extra struct {

	// idc
	Idc string `json:"idc,omitempty"`

	// rack
	Rack string `json:"rack,omitempty"`

	// room
	Room string `json:"room,omitempty"`

	// site
	Site string `json:"site,omitempty"`
}

// Validate validates this extra
func (m *Extra) Validate(formats strfmt.Registry) error {
	return nil
}

// MarshalBinary interface implementation
func (m *Extra) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *Extra) UnmarshalBinary(b []byte) error {
	var res Extra
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
