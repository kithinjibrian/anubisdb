package engine

import (
	"fmt"

	"github.com/kithinjibrian/anubisdb/internal/catalog"
	"github.com/kithinjibrian/anubisdb/internal/parser"
	"github.com/kithinjibrian/anubisdb/internal/storage"
)

type Engine struct {
	catalog *catalog.Catalog
	storage *storage.Storage
	planner *Planner
}

func NewEngine(dbFile string) (*Engine, error) {
	store, err := storage.NewStorage(dbFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open storage: %w", err)
	}

	cat, err := catalog.NewCatalog(store.Pager)
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("failed to initialize catalog: %w", err)
	}

	return &Engine{
		catalog: cat,
		storage: store,
		planner: NewPlanner(cat),
	}, nil
}

func (e *Engine) Close() error {
	if err := e.storage.Close(); err != nil {
		return fmt.Errorf("failed to close storage: %w", err)
	}
	return nil
}

func (e *Engine) Execute(node parser.Node) string {

	plan, err := e.planner.Plan(node)
	if err != nil {
		return formatError(err)
	}

	result, err := ExecutePlan(e, plan)
	if err != nil {
		return formatError(err)
	}

	return result
}

func formatError(err error) string {
	return fmt.Sprintf("Error: %s", err.Error())
}
