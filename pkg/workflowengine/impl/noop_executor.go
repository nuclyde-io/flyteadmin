package impl

import (
	"context"
	"fmt"
	"github.com/NYTimes/gizmo/pubsub"
	runtimeInterfaces "github.com/lyft/flyteadmin/pkg/runtime/interfaces"
	"github.com/lyft/flyteadmin/pkg/workflowengine/interfaces"
	"github.com/lyft/flytestdlib/logger"
)

type NoopExecutor struct {
	config      runtimeInterfaces.NamespaceMappingConfiguration
	builder     interfaces.FlyteWorkflowInterface
	publisher   pubsub.Publisher
	roleNameKey string
}

func (c *NoopExecutor) ExecuteWorkflow(ctx context.Context, input interfaces.ExecuteWorkflowInput) (*interfaces.ExecutionInfo, error) {
	return &interfaces.ExecutionInfo{}, nil
}

func (c *NoopExecutor) ExecuteTask(ctx context.Context, input interfaces.ExecuteTaskInput) (*interfaces.ExecutionInfo, error) {
	return nil, fmt.Errorf("not yet implemented")
}

func (c *NoopExecutor) TerminateWorkflowExecution(
	ctx context.Context, input interfaces.TerminateWorkflowInput) error {
	logger.Debugf(ctx, "Noop executor, terminator will not do anything")
	return nil
}
