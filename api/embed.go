// Package api embeds the OpenAPI specification served at /openapi.yaml.
package api

import _ "embed"

//go:embed openapi.yaml
var Spec []byte
