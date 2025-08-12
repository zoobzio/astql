module github.com/zoobzio/astql

go 1.23.1

toolchain go1.24.5

require (
	github.com/zoobzio/sentinel v0.0.0-00010101000000-000000000000
	github.com/zoobzio/zlog v0.0.0
)

require (
	github.com/zoobzio/pipz v0.6.0 // indirect
	golang.org/x/time v0.12.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/zoobzio/zlog => ../zlog

replace github.com/zoobzio/pipz => ../pipz

replace github.com/zoobzio/sentinel => ../sentinel
