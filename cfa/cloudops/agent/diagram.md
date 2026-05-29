# LangChain Agent Workflow Sequence Diagram

```mermaid
sequenceDiagram
    participant User as User/Client
    participant Agent as CloudClientAgent
    participant LLM as ChatOpenAI<br/>(LLM)
    participant LC as LangChain<br/>Agent Executor
    participant DepLoader as DependencyConfigLoader
    participant WFPlanner as WorkflowPlanner
    participant WFExecutor as WorkflowExecutor
    participant StateTrack as ExecutionState
    participant CC as CloudClient
    participant Azure as Azure Batch/Storage

    User->>Agent: run(user_input)
    activate Agent

    Agent->>LC: invoke({"input": user_input})
    activate LC

    LC->>LLM: generate_message(prompt + input)
    activate LLM
    LLM->>LLM: Parse user intent
    LLM->>LLM: Identify operations needed
    LLM-->>LC: tool_calls: [operation1, operation2]
    deactivate LLM

    Note over LC: LangChain processes<br/>tool calls and loops

    LC->>Agent: get_list_operations_tool()
    activate Agent
    Agent->>DepLoader: get_operations()
    activate DepLoader
    DepLoader-->>Agent: ["create_pool", "create_job", ...]
    deactivate DepLoader
    Agent-->>LC: Available operations
    deactivate Agent

    LC->>LLM: Re-analyze with available ops
    activate LLM
    LLM->>LLM: Determine workflow sequence
    LLM-->>LC: ["create_pool", "create_job", "add_task"]
    deactivate LLM

    LC->>Agent: plan_workflow_tool(operations)
    activate Agent

    Agent->>WFPlanner: plan(["create_pool", "create_job", "add_task"])
    activate WFPlanner

    WFPlanner->>DepLoader: get_all_dependencies("add_task")
    activate DepLoader
    DepLoader-->>WFPlanner: {"create_job", "create_pool"}
    deactivate DepLoader

    WFPlanner->>DepLoader: validate_operation_sequence(ops)
    activate DepLoader
    DepLoader->>DepLoader: Check for cycles
    DepLoader->>DepLoader: Verify dependencies met
    DepLoader-->>WFPlanner: {valid: true, message: "OK"}
    deactivate DepLoader

    WFPlanner->>DepLoader: get_optimal_operation_order(ops)
    activate DepLoader
    DepLoader->>DepLoader: Topological sort
    DepLoader-->>WFPlanner: ["create_pool", "create_job", "add_task"]
    deactivate DepLoader

    WFPlanner->>WFPlanner: Build execution plan
    WFPlanner->>WFPlanner: Extract parameters
    WFPlanner-->>Agent: ExecutionPlan{ops, order, params}
    deactivate WFPlanner

    Agent->>StateTrack: initialize()
    activate StateTrack
    StateTrack-->>Agent: ExecutionState()
    deactivate StateTrack

    Agent-->>LC: ExecutionPlan + ready
    deactivate Agent

    loop For each operation in plan
        LC->>LLM: confirm_operation(operation, params)
        activate LLM
        LLM-->>LC: approved
        deactivate LLM

        LC->>Agent: execute_operation(op, params)
        activate Agent

        Agent->>WFExecutor: execute(operation, params, state)
        activate WFExecutor

        WFExecutor->>DepLoader: get_dependencies(operation)
        activate DepLoader
        DepLoader-->>WFExecutor: required_deps
        deactivate DepLoader

        WFExecutor->>StateTrack: check_requirements_met(deps)
        activate StateTrack
        StateTrack-->>WFExecutor: all_met: true
        deactivate StateTrack

        WFExecutor->>DepLoader: validate_parameters(operation, params)
        activate DepLoader
        DepLoader-->>WFExecutor: {valid: true}
        deactivate DepLoader

        alt Parameters Valid
            WFExecutor->>CC: call_operation(operation, params)
            activate CC

            par Pool Operations
                Note over CC: Pool Operations
                CC->>Azure: create_pool_request()
                activate Azure
                Azure-->>CC: pool_id
                deactivate Azure

                CC->>StateTrack: track_resource("pool", pool_name)
                activate StateTrack
                StateTrack->>StateTrack: pools_created.append(pool_name)
                StateTrack-->>CC: OK
                deactivate StateTrack

            and Job Operations
                Note over CC: Job Operations
                CC->>Azure: create_job_request()
                activate Azure
                Azure-->>CC: job_id
                deactivate Azure

                CC->>StateTrack: track_resource("job", job_name)
                activate StateTrack
                StateTrack->>StateTrack: jobs_created.append(job_name)
                StateTrack-->>CC: OK
                deactivate StateTrack

            and Task Operations
                Note over CC: Task Operations
                CC->>Azure: add_task_request()
                activate Azure
                Azure-->>CC: task_id
                deactivate Azure

                CC->>StateTrack: track_resource("task", task_id)
                activate StateTrack
                StateTrack->>StateTrack: tasks_added[job].append(task_id)
                StateTrack-->>CC: OK
                deactivate StateTrack
            end

            CC-->>WFExecutor: result
            deactivate CC

            WFExecutor->>StateTrack: record_operation(op, params, result, "success")
            activate StateTrack
            StateTrack->>StateTrack: operations_executed.append(record)
            StateTrack-->>WFExecutor: OK
            deactivate StateTrack

            WFExecutor-->>Agent: ExecutionResult{success, result}

        else Parameters Invalid
            WFExecutor->>StateTrack: record_operation(op, params, null, "error")
            activate StateTrack
            StateTrack-->>WFExecutor: OK
            deactivate StateTrack

            WFExecutor-->>Agent: ExecutionResult{error, message}
        end

        deactivate WFExecutor

        Agent->>LC: operation_result
        deactivate Agent

        LC->>LLM: process_result(result, next_step)
        activate LLM
        LLM->>LLM: Decide next action
        LLM-->>LC: continue/error/complete
        deactivate LLM
    end

    LC->>Agent: final_summary()
    activate Agent
    Agent->>StateTrack: get_summary()
    activate StateTrack
    StateTrack-->>Agent: ExecutionSummary
    deactivate StateTrack

    Agent-->>LC: summary
    deactivate Agent

    LC-->>Agent: Final response
    deactivate LC

    Agent-->>User: Workflow completed successfully
    deactivate Agent
