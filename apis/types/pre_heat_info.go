// Code generated by go-swagger; DO NOT EDIT.

package types

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/swag"
)

// PreHeatInfo The object shows the info of preheat.
// The authentication or other external information should be inclueded in url and headers.
//
// swagger:model PreHeatInfo
type PreHeatInfo struct {

	// headers
	Headers []string `json:"headers"`

	// seed task ID
	SeedTaskID string `json:"seedTaskID,omitempty"`

	// The file location
	URL string `json:"url,omitempty"`
}

// Validate validates this pre heat info
func (m *PreHeatInfo) Validate(formats strfmt.Registry) error {
	return nil
}

// MarshalBinary interface implementation
func (m *PreHeatInfo) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *PreHeatInfo) UnmarshalBinary(b []byte) error {
	var res PreHeatInfo
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
