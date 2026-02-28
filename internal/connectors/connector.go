package connectors

import "context"

type Connector interface {
	Name() string
	Start(ctx context.Context) error
	Stop() error
	Health() error
}
