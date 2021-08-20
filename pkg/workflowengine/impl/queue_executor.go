package impl

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/NYTimes/gizmo/pubsub"
	"github.com/lyft/flyteadmin/pkg/common"
	"github.com/lyft/flyteadmin/pkg/errors"
	runtimeInterfaces "github.com/lyft/flyteadmin/pkg/runtime/interfaces"
	"github.com/lyft/flyteadmin/pkg/workflowengine/interfaces"
	"github.com/lyft/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	"github.com/lyft/flytestdlib/logger"
	"google.golang.org/grpc/codes"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type QueueExecutor struct {
	config      runtimeInterfaces.NamespaceMappingConfiguration
	builder     interfaces.FlyteWorkflowInterface
	publisher   pubsub.Publisher
	roleNameKey string
}

func (c *QueueExecutor) ExecuteWorkflow(ctx context.Context, input interfaces.ExecuteWorkflowInput) (*interfaces.ExecutionInfo, error) {
	namespace := common.GetNamespaceName(c.config.GetNamespaceMappingConfig(), input.ExecutionID.GetProject(), input.ExecutionID.GetDomain())
	flyteWf, err := c.builder.BuildFlyteWorkflow(&input.WfClosure, input.Inputs, input.ExecutionID, namespace)
	if err != nil {
		logger.Infof(ctx, "failed to build the workflow [%+v] %v",
			input.WfClosure.Primary.Template.Id, err)
		return nil, errors.NewFlyteAdminErrorf(codes.Internal, "failed to build the workflow [%+v] %v",
			input.WfClosure.Primary.Template.Id, err)
	}

	// add the executionId so Propeller can send events back that are associated with the ID
	flyteWf.ExecutionID = v1alpha1.WorkflowExecutionIdentifier{
		WorkflowExecutionIdentifier: input.ExecutionID,
	}
	// add the acceptedAt timestamp so propeller can emit latency metrics.
	acceptAtWrapper := v1.NewTime(input.AcceptedAt)
	flyteWf.AcceptedAt = &acceptAtWrapper

	addPermissions(input.Reference, flyteWf, c.roleNameKey)

	labels := addMapValues(input.Labels, flyteWf.Labels)
	flyteWf.Labels = labels
	annotations := addMapValues(input.Annotations, flyteWf.Annotations)
	flyteWf.Annotations = annotations
	if flyteWf.WorkflowMeta == nil {
		flyteWf.WorkflowMeta = &v1alpha1.WorkflowMeta{}
	}
	flyteWf.WorkflowMeta.EventVersion = 1
	addExecutionOverrides(input.TaskPluginOverrides, flyteWf)

	if input.Reference.Spec.RawOutputDataConfig != nil {
		flyteWf.RawOutputDataConfig = v1alpha1.RawOutputDataConfig{
			RawOutputDataConfig: input.Reference.Spec.RawOutputDataConfig,
		}
	}

	/*
		TODO(katrogan): uncomment once propeller has updated the flyte workflow CRD.
		queueingBudgetSeconds := int64(input.QueueingBudget.Seconds())
		flyteWf.QueuingBudgetSeconds = &queueingBudgetSeconds
	*/

	j, err := json.Marshal(flyteWf)
	if err != nil {
		logger.Infof(ctx, "Failed to serialize Json")
		return nil, err
	}

	key := fmt.Sprintf("c-%s-%s", namespace, input.ExecutionID.Name)
	err = c.publisher.PublishRaw(ctx, key, j)
	if err != nil {
		logger.Errorf(ctx, "Failed to publish workflow to execution queue")
		return nil, err
	}

	logger.Debugf(ctx, "Successfully created workflow execution [%+v]", input.WfClosure.Primary.Template.Id)

	return &interfaces.ExecutionInfo{}, nil
}

func (c *QueueExecutor) ExecuteTask(ctx context.Context, input interfaces.ExecuteTaskInput) (*interfaces.ExecutionInfo, error) {
	return nil, fmt.Errorf("not yet implemented")
}

func (c *QueueExecutor) TerminateWorkflowExecution(
	ctx context.Context, input interfaces.TerminateWorkflowInput) error {
	if input.ExecutionID == nil {
		return errors.NewFlyteAdminErrorf(codes.Internal, "invalid execution id")
	}
	namespace := common.GetNamespaceName(c.config.GetNamespaceMappingConfig(), input.ExecutionID.GetProject(), input.ExecutionID.GetDomain())
	key := fmt.Sprintf("d-%s-%s", namespace, input.ExecutionID.Name)
	j, err := json.Marshal(DeleteMessage{
		DummyField: 100,
		Name: input.ExecutionID.Name,
		Namespace: namespace,
	})
	if err != nil {
		logger.Errorf(ctx, "failed to delete workflow, %s", err)
		return err
	}
	err = c.publisher.PublishRaw(ctx, key, j)
	if err != nil {
		logger.Errorf(ctx, "Failed to publish delete message, err: %s", err)
		return err
	}
	logger.Debugf(ctx, "terminated execution: %v in cluster %s", input.ExecutionID, input.Cluster)
	return nil
}

type DeleteMessage struct {
	// The following field exists just to disambiguate the 2 messages
	DummyField int `json:"dummy_field"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}
