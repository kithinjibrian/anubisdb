package engine

import (
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
		return nil, err
	}

	cat, err := catalog.NewCatalog(store.Pager)

	if err != nil {
		return nil, err
	}

	return &Engine{
		catalog: cat,
		storage: store,
		planner: NewPlanner(cat),
	}, nil
}

func (e *Engine) Close() error {
	return e.storage.Close()
}

func (e *Engine) Execute(node parser.Node) string {
	plan, err := e.planner.Plan(node)
	if err != nil {
		return "Error: " + err.Error()
	}

	result, err := ExecutePlan(e, plan)
	if err != nil {
		return "Error: " + err.Error()
	}

	return result
}
