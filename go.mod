module github.com/dhiaayachi/multiraft

go 1.22.4

require (
	github.com/hashicorp/go-hclog v1.6.2
	github.com/hashicorp/go-memdb v1.3.4
	github.com/hashicorp/go-msgpack/v2 v2.1.2
	github.com/hashicorp/raft v1.7.0
	github.com/stretchr/testify v1.8.4
)

replace github.com/hashicorp/raft v1.7.0 => ../raft

require (
	github.com/armon/go-metrics v0.4.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/fatih/color v1.13.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/objx v0.5.0 // indirect
	golang.org/x/sys v0.13.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
