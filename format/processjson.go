// Copyright 2015-2016 trivago GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package format

import (
	"encoding/json"
	"github.com/trivago/gollum/core"
	"github.com/trivago/gollum/core/log"
	"github.com/trivago/gollum/shared"
	"strings"
	"time"
)

// ProcessJSON formatter plugin
// ProcessJSON is a formatter that allows modifications to fields of a given
// JSON message. The message is modified and returned again as JSON.
// Configuration example
//
//  - "stream.Broadcast":
//    Formatter: "format.processJSON"
//    ProcessJSONDataFormatter: "format.Forward"
//    ProcessJSONDirectives:
//      - "host:split: :host:@timestamp"
//      - "@timestamp:time:20060102150405:2006-01-02 15\\:04\\:05"
//      - "error:replace:°:\n"
//      - "text:trim: \t"
//      - "foo:rename:bar"
//    ProcessJSONTrimValues: true
//
// ProcessJSONDataFormatter formatter that will be applied before
// ProcessJSONDirectives are processed.
//
// ProcessJSONDirectives defines the action to be applied to the json payload.
// Directives are processed in order of appearance.
// The directives have to be given in the form of key:operation:parameters, where
// operation can be one of the following:
// - split:<string>:{<key>:<key>:...} Split the value by a string and set the
// resulting array elements to the given fields in order of appearance.
// - replace:<old>:<new> replace a given string in the value with a new one
// - trim:<characters> remove the given characters (not string!) from the start
// and end of the value
// - rename:<old>:<new> rename a given field
// - timestamp:<read>:<write> read a timestamp and transform it into another
// format
//
// ProcessJSONTrimValues will trim whitspaces from all values if enabled.
// Enabled by default.
type ProcessJSON struct {
	base       core.Formatter
	directives []transformDirective
	trimValues bool
}

type transformDirective struct {
	key        string
	operation  string
	parameters []string
}

type valueMap map[string]string

func init() {
	shared.TypeRegistry.Register(ProcessJSON{})
}

// Configure initializes this formatter with values from a plugin config.
func (format *ProcessJSON) Configure(conf core.PluginConfig) error {
	plugin, err := core.NewPluginWithType(conf.GetString("ProcessJSONDataFormatter", "format.Forward"), conf)
	if err != nil {
		return err
	}
	directives := conf.GetStringArray("ProcessJSONDirectives", []string{})

	format.base = plugin.(core.Formatter)
	format.directives = make([]transformDirective, 0, len(directives))
	format.trimValues = conf.GetBool("ProcessJSONTrimValues", true)

	for _, directive := range directives {
		directive := strings.Replace(directive, "\\:", "\r", -1)
		parts := strings.Split(directive, ":")
		for i, value := range parts {
			parts[i] = strings.Replace(value, "\r", ":", -1)
		}

		if len(parts) >= 2 {
			newDirective := transformDirective{
				key:       parts[0],
				operation: strings.ToLower(parts[1]),
			}

			for i := 2; i < len(parts); i++ {
				newDirective.parameters = append(newDirective.parameters, shared.Unescape(parts[i]))
			}

			format.directives = append(format.directives, newDirective)
		}
	}

	return nil
}

func (values *valueMap) processDirective(directive transformDirective) {
	if value, keyExists := (*values)[directive.key]; keyExists {

		numParameters := len(directive.parameters)
		switch directive.operation {
		case "rename":
			if numParameters == 1 {
				(*values)[shared.Unescape(directive.parameters[0])] = value
				delete(*values, directive.key)
			}

		case "time":
			if numParameters == 2 {

				if timestamp, err := time.Parse(directive.parameters[0], value[:len(directive.parameters[0])]); err != nil {
					Log.Warning.Print("ProcessJSON failed to parse a timestamp: ", err)
				} else {
					(*values)[directive.key] = timestamp.Format(directive.parameters[1])
				}
			}

		case "split":
			if numParameters > 1 {
				token := shared.Unescape(directive.parameters[0])
				if strings.Contains(value, token) {
					elements := strings.Split(value, token)
					mapping := directive.parameters[1:]
					maxItems := shared.MinI(len(elements), len(mapping))

					for i := 0; i < maxItems; i++ {
						(*values)[mapping[i]] = elements[i]
					}
				}
			}

		case "replace":
			if numParameters == 2 {
				(*values)[directive.key] = strings.Replace(value, shared.Unescape(directive.parameters[0]), shared.Unescape(directive.parameters[1]), -1)
			}

		case "trim":
			switch {
			case numParameters == 0:
				(*values)[directive.key] = strings.Trim(value, " \t")
			case numParameters == 1:
				(*values)[directive.key] = strings.Trim(value, shared.Unescape(directive.parameters[0]))
			}

		case "remove":
			delete(*values, directive.key)
		}
	}
}

// Format modifies the JSON payload of this message
func (format *ProcessJSON) Format(msg core.Message) ([]byte, core.MessageStreamID) {
	data, streamID := format.base.Format(msg)
	if len(format.directives) == 0 {
		return data, streamID // ### return, no directives ###
	}

	values := make(valueMap)
	err := json.Unmarshal(data, &values)
	if err != nil {
		Log.Warning.Print("ProcessJSON failed to unmarshal a message: ", err)
		return data, streamID // ### return, malformed data ###
	}

	for _, directive := range format.directives {
		values.processDirective(directive)
	}

	if format.trimValues {
		for key, value := range values {
			values[key] = strings.Trim(value, " ")
		}
	}

	if jsonData, err := json.Marshal(values); err == nil {
		return jsonData, streamID // ### return, ok ###
	}

	Log.Warning.Print("ProcessJSON failed to marshal a message: ", err)
	return data, streamID
}
