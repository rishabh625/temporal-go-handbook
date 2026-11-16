# Temporal Learning Notes

A structured reference for learning the Temporal Platform, with examples and terminology focused primarily on the Go SDK.

> **Note:** These are personal learning notes. Verify version-specific limits and APIs against the current Temporal documentation before using them in production.

## Table of Contents

<details markdown="1">
<summary><strong>Click to expand Table of Contents</strong></summary>

1. [Core Concepts](#1-core-concepts)
2. [Temporal Application Architecture](#2-temporal-application-architecture)
3. [Workflows](#3-workflows)
4. [Activities](#4-activities)
5. [Workers and Task Queues](#5-workers-and-task-queues)
6. [Interacting with Workflow Executions](#6-interacting-with-workflow-executions)
7. [Visibility and Search Attributes](#7-visibility-and-search-attributes)
8. [Timeouts, Heartbeats, and Cancellation](#8-timeouts-heartbeats-and-cancellation)
9. [Failure Handling and Retry Policies](#9-failure-handling-and-retry-policies)
10. [Payload Conversion, Compression, and Encryption](#10-payload-conversion-compression-and-encryption)
11. [Workflow IDs and Retention](#11-workflow-ids-and-retention)
12. [Timers and Long-Running Workflows](#12-timers-and-long-running-workflows)
13. [Event History and Continue-As-New](#13-event-history-and-continue-as-new)
14. [Sticky Execution](#14-sticky-execution)
15. [Workflow Determinism and Versioning](#15-workflow-determinism-and-versioning)
16. [Temporal Cluster Services](#16-temporal-cluster-services)
17. [Polyglot Development](#17-polyglot-development)
18. [Debugging Notes](#18-debugging-notes)
19. [Quick Reference](#19-quick-reference)

</details>

---

## 1. Core Concepts

### Workflow

A **Workflow** is a durable function that defines a sequence of steps for a business process. A Workflow can be invoked multiple times and must behave deterministically during replay.

For the same Workflow history, the Workflow code must produce the same sequence of Temporal commands. External calls and other non-deterministic operations should not be performed directly inside Workflow code.

### Activity

An **Activity** represents work that may be non-deterministic or interact with an external system, such as:

- Calling an API or LLM
- Reading or writing files
- Accessing a database
- Sending an email
- Processing a payment

Activities can be retried and timed out independently from the Workflow.

### Worker

A **Worker** is a process that hosts and executes Workflow and Activity code. Workers continuously poll a Task Queue and accept tasks when they have available capacity.

### Task Queue

A **Task Queue** connects the Temporal Cluster to Workers. The cluster places Workflow Tasks and Activity Tasks on a queue, and Workers poll that queue to execute them.

### Temporal Client

A **Temporal Client** starts and interacts with Workflow Executions. It can:

- Start a Workflow
- Send a Signal
- execute a Query
- Request cancellation or termination
- Retrieve a Workflow result

### Temporal Cluster

The **Temporal Cluster** manages orchestration and durable execution. It preserves Workflow state through Event History and coordinates task delivery through Task Queues.

---

## 2. Temporal Application Architecture

A typical Temporal application contains:

1. A **Workflow Definition**
2. One or more **Activity Definitions**
3. A **Worker configuration**
4. A **Client application**
5. A **Temporal Cluster**

A simplified execution flow is:

```text
Client application
       |
       | Start Workflow / Signal / Query
       v
Temporal Frontend Service
       |
       v
Temporal Cluster and Task Queue
       |
       | Worker polls
       v
Worker executes Workflow and Activity code
```

The Temporal Frontend Service acts as an API gateway. The Web UI, CLI, and SDK clients connect to it, normally over gRPC.

---

## 3. Workflows

A Workflow defines the orchestration logic for a durable process. It may execute multiple Activities sequentially or in parallel.

### Registering a Workflow in Go

A Workflow Definition is registered with a Worker:

```go
w := worker.New(client, "my-task-queue", worker.Options{})
w.RegisterWorkflow(MyWorkflow)

err := w.Run(worker.InterruptCh())
if err != nil {
    log.Fatal(err)
}
```

### Workflow Input

In Go, using a struct for Workflow input makes future changes easier to manage:

```go
type WorkflowInput struct {
    OrderID string
    Email   string
}
```

Fields can be added more safely than changing a long list of positional parameters. However, data-contract compatibility must still be considered for running and replaying Workflow Executions.

### Workflow Errors

If a Workflow returns an error, the Workflow Execution is marked as failed. Unlike Activities, Workflows do not automatically use an Activity-style Retry Policy unless retry behavior is explicitly configured when starting the Workflow.

---

## 4. Activities

An Activity Definition is typically an exported Go function. Its parameters and return values must be serializable by the configured Data Converter.

```go
func ChargeCard(ctx context.Context, input PaymentInput) (ChargeStatus, error) {
    // Call an external payment service.
    return ChargeStatus{Success: true}, nil
}
```

### Registering an Activity

```go
w.RegisterActivity(ChargeCard)
```

### Executing an Activity from a Workflow

```go
options := workflow.ActivityOptions{
    StartToCloseTimeout: 30 * time.Second,
}

ctx = workflow.WithActivityOptions(ctx, options)

var result ChargeStatus
err := workflow.ExecuteActivity(ctx, ChargeCard, input).Get(ctx, &result)
if err != nil {
    return err
}
```

An Activity must be invoked by Workflow code. A Worker that has the Activity registered must poll the relevant Task Queue.

### Asynchronous Activity Completion

Asynchronous Activity Completion is useful when an Activity delegates work to an external system and the result will arrive later. The Activity can return without completing, and the external system can complete it later by using its Activity task token.

Use a Signal instead when the interaction is better represented as an external message that changes or advances the Workflow state.

---

## 5. Workers and Task Queues

Workers poll Task Queues and execute application code. Task Queues are created dynamically when first used.

### Availability Recommendation

For production workloads, run at least two Worker processes for each important Task Queue. If one Worker crashes, another Worker can continue polling and process new or retried tasks.

### Responsibilities

| Component | Responsibility |
|---|---|
| Client | Requests Workflow execution and retrieves results |
| Temporal Cluster | Orchestrates durable execution |
| Task Queue | Routes tasks from the cluster to Workers |
| Worker | Executes Workflow and Activity code |

---

## 6. Interacting with Workflow Executions

### Signals

A **Signal** is an asynchronous message sent to a running Workflow Execution. It can update state or control the execution.

Example: an order-processing Workflow receives a `PaymentReceived` Signal and then proceeds to ship the order.

Signals are:

- Defined and handled in Workflow code
- Sent from a Temporal Client, CLI, or another Workflow
- Asynchronous

Workflows can also send Signals to other Workflows. These are commonly called **External Signals**.

### Signal-With-Start

Sending a normal Signal requires a Workflow Execution to exist. **Signal-With-Start** handles the case where the Workflow might not be running:

- If the Workflow is running, Temporal sends the Signal.
- If the Workflow is not running, Temporal starts it and then sends the Signal.

### Signal Constraints

The notes record these important limits:

- Up to **2,000 pending outgoing Signal requests** per Workflow Execution
- Up to **10,000 received Signals** during a single Workflow Execution
- Event History limits still apply

Batching related messages into one Signal can reduce Signal and Event History growth.

> Limits can change by Temporal version or service configuration. Verify them in the current documentation.

### Queries

A **Query** synchronously reads the internal state of a Workflow Execution without changing it.

Typical uses include:

- Reading the current order status
- Reporting completion percentage
- Checking how many Activities have completed
- Inspecting state before a long-running Workflow finishes

Queries may also be issued against a closed Workflow Execution when a compatible Worker is available to process the Query.

### Signals vs. Queries

| Feature | Signal | Query |
|---|---|---|
| Purpose | Change Workflow state | Read Workflow state |
| Operation | Asynchronous | Synchronous |
| Mutates state | Yes | No |
| Requires Workflow code | Signal handler | Query handler |

---

## 7. Visibility and Search Attributes

**Visibility** refers to the Temporal subsystems and APIs used to view, filter, and search Workflow Executions.

The following use Visibility data:

- Temporal Web UI
- Temporal CLI
- Temporal SDKs

### List Filter

A SQL-like List Filter can be used to find Workflow Executions:

```text
WorkflowType = "pizzaWorkflow" AND ExecutionStatus = "Running"
```

CLI example:

```bash
temporal workflow list \
  --query 'WorkflowType = "pizzaWorkflow" AND ExecutionStatus = "Running"'
```

### Search Attributes

Search Attributes organize Workflow Executions and make them searchable.

Examples of default Search Attributes include:

- `WorkflowType`
- `ExecutionStatus`
- `StartTime`
- `CloseTime`

Custom Search Attributes are configured per Namespace. Supported types include:

- `Bool`
- `Datetime`
- `Double`
- `Int`
- `Keyword`
- `KeywordList`
- `Text`

---

## 8. Timeouts, Heartbeats, and Cancellation

### Activity Heartbeats

An Activity Heartbeat is a periodic progress notification sent from a long-running Activity to the Temporal Service.

Heartbeats help Temporal:

- Detect an unresponsive Activity
- Record Activity progress
- Deliver cancellation requests
- Allow retries to resume from recorded progress

The notes record a default heartbeat throttle interval of **30 seconds** and a maximum throttle interval of **60 seconds**, both configurable through Worker options.

### Activity Timeouts

#### Start-to-Close Timeout

The maximum time allowed for a single Activity execution attempt, from when a Worker starts it until that attempt completes.

#### Schedule-to-Close Timeout

The maximum total time from when an Activity is scheduled until it completes, including queueing time and retries.

#### Schedule-to-Start Timeout

The maximum time an Activity Task may wait in a Task Queue before a Worker starts it.

#### Heartbeat Timeout

The maximum permitted time between Activity Heartbeats. It is useful for detecting stalled long-running Activities.

Activity timeouts are configured using `workflow.ActivityOptions`:

```go
options := workflow.ActivityOptions{
    StartToCloseTimeout:    30 * time.Second,
    ScheduleToCloseTimeout: 5 * time.Minute,
    HeartbeatTimeout:       10 * time.Second,
}
```

At least one of `StartToCloseTimeout` or `ScheduleToCloseTimeout` must be configured.

### Workflow Cancellation

Cancellation requests a graceful stop and gives Workflow and Activity code an opportunity to perform cleanup.

### Workflow Termination

Termination stops a Workflow Execution immediately. It does not allow the Workflow to run cleanup logic.

---

## 9. Failure Handling and Retry Policies

### General Failure Categories

Failures can be considered:

1. **Transient**: temporary and likely to succeed quickly on retry
2. **Intermittent**: occurs unpredictably and may succeed after retry
3. **Permanent**: should not be retried without changing the input or code

### Temporal Failure Types

Common failure types include:

- `ApplicationFailure`
- `CanceledFailure`
- `TerminatedFailure`
- `TimeoutFailure`
- `ServerFailure`
- `ChildWorkflowFailure`
- `ActivityFailure`
- `WorkflowExecutionAlreadyStarted`

### Default Activity Retry Policy

Temporal retries failed Activities according to their Retry Policy.

| Setting | Recorded default | Meaning |
|---|---:|---|
| Initial Interval | 1 second | Delay before the first retry |
| Backoff Coefficient | 2.0 | Multiplier for subsequent delays |
| Maximum Interval | 100 × initial interval | Upper bound for retry delay |
| Maximum Attempts | 0 | Interpreted as unlimited attempts |
| Non-Retryable Errors | None | Error types that must not be retried |

With an initial interval of one second and a backoff coefficient of `2.0`, delays grow approximately as follows:

```text
1s, 2s, 4s, 8s, 16s, 32s, 64s, 100s, 100s, ...
```

### Non-Retryable Application Error in Go

```go
if len(address.CardNumber) != 16 {
    return chargeStatus, temporal.NewNonRetryableApplicationError(
        "Credit card number must contain 16 digits",
        "CreditCardError",
        nil,
    )
}
```

Use a non-retryable error when retrying cannot resolve the problem, such as invalid input.

---

## 10. Payload Conversion, Compression, and Encryption

Temporal Data Converters serialize and deserialize data sent between:

- Clients
- Workflows
- Activities
- The Temporal Service

Payloads may represent JSON, Protocol Buffers, byte arrays, or null values.

### Custom Payload Codec

A custom Payload Codec can provide:

- Encryption and decryption
- Compression and decompression
- Custom handling for payload representations

Encryption keys should be stored in a secure key-management system such as HashiCorp Vault, AWS KMS, Google Cloud KMS, or Azure Key Vault rather than embedded in application code.

### Remote Codec Server

The Temporal Web UI and CLI can call a remote Codec Server to decode payloads. A decode endpoint may resemble:

```text
POST http://codec.server/your-namespace/decode
```

The request and response contain a JSON object with a `payloads` array. The server processes those payloads through the configured Payload Codec before returning them.

> Protect a Codec Server with authentication, authorization, TLS, and network controls because it can access sensitive plaintext data.

---

## 11. Workflow IDs and Retention

### Namespaces

A Namespace provides logical isolation within a Temporal Cluster. Different teams or applications can own separate Namespaces.

### Workflow ID

A Workflow ID identifies a Workflow Execution within a Namespace. Temporal manages uniqueness according to the configured ID conflict and reuse policies.

The notes list the following Workflow ID Reuse Policy values:

- `WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE`
- `WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY`
- `WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE`
- `WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING`

> Temporal SDKs and API versions may distinguish Workflow ID Reuse Policy from Workflow ID Conflict Policy. Check the current SDK documentation when configuring new executions.

### Retention Period

After a Workflow Execution closes, its history is retained according to the Namespace retention period. The notes indicate a typical range of **1 to 30 days**.

Retention applies after a Workflow closes. It does not limit how long an active Workflow may run.

Self-hosted clusters can configure archival for longer-term storage.

---

## 12. Timers and Long-Running Workflows

Temporal Timers allow Workflow code to wait durably without consuming a Worker thread for the entire waiting period. Timer state is maintained by the Temporal Service.

Typical use case:

- Send an onboarding email after one day
- Send a follow-up email after one week

### Sleep in Go

```go
err := workflow.Sleep(ctx, 24*time.Hour)
if err != nil {
    return err
}
```

### Timer in Go

```go
timer := workflow.NewTimer(ctx, 24*time.Hour)
if err := timer.Get(ctx, nil); err != nil {
    return err
}
```

Use Temporal's Workflow APIs instead of standard time or sleep functions so that timer behavior is recorded and replay-safe.

---

## 13. Event History and Continue-As-New

A Workflow Execution's **Event History** records what happened during the execution, including commands, task transitions, Signals, timers, and Activity results.

Every event includes at least:

- Event ID
- Event time
- Event type

### Recorded Limits

The notes record:

- Warning around **10,240 events**
- Termination at **51,200 events**
- Maximum history size of approximately **50 MB**
- Recommended operating target below approximately **10,000 events or 10 MB**

> Verify exact limits for the target Temporal Cloud account or self-hosted cluster.

### Continue-As-New

When a Workflow has a large or continually growing history, use **Continue-As-New**. It closes the current run and starts a new run with a fresh Event History while preserving the logical Workflow ID.

Common uses include:

- Entity Workflows
- Polling loops
- High-volume Signal processing
- Periodic processing that has no natural end

---

## 14. Sticky Execution

Sticky Execution attempts to route subsequent Workflow Tasks to the same Worker that processed earlier tasks for that Workflow Execution.

The Temporal Service creates a Worker-specific **Sticky Queue**. Reusing the same Worker can improve performance because the Worker may retain the Workflow's in-memory state and avoid a complete replay.

If the Worker does not start the Workflow Task from the Sticky Queue within the configured timeout, recorded here as five seconds by default, Temporal disables stickiness for that task and reschedules it on the original Task Queue.

---

## 15. Workflow Determinism and Versioning

### History Replay

Temporal recovers Workflow state by replaying Event History. During replay, Workflow code must generate commands that are compatible with the recorded events.

Examples of command-to-event relationships include:

```text
Execute Activity
  -> ActivityTaskScheduled
  -> ActivityTaskStarted
  -> ActivityTaskCompleted

Start Timer
  -> TimerStarted
  -> TimerFired or TimerCanceled
```

### Non-Deterministic Code

A Workflow can become non-deterministic if it directly uses:

- Random values
- Current system time
- Direct network or database calls
- Mutable global state
- Non-deterministic iteration order

For example, conditionally scheduling a timer based on an unrecorded random number could cause replay to produce a different command sequence.

### Code Changes That May Cause Non-Determinism

Potentially unsafe changes include:

- Removing or reordering an Activity command
- Adding or removing a timer in an already-executed path
- Changing certain Activity Options
- Changing a Retry Policy in a replay-incompatible way
- Modifying branching logic for existing executions

Use Temporal's versioning mechanisms and Worker Deployment features to maintain compatibility for running Workflow Executions.

### Workflow Static Analysis for Go

The Workflow Check tool can help detect some non-deterministic Go code:

```bash
go install go.temporal.io/sdk/contrib/tools/workflowcheck@latest
```

### Reset

If a bad deployment produces non-deterministic failures, Temporal Reset can create a new run from a selected point in the Event History.

Reset does not simply continue from the Worker's current in-memory position. Understand the selected reset point and the possibility of repeated side effects before using it.

---

## 16. Temporal Cluster Services

A self-hosted Temporal Cluster includes these main services:

### Frontend Service

- Receives SDK, CLI, and Web UI requests
- Validates and routes inbound calls
- Acts as the API gateway for the cluster

### History Service

- Maintains Workflow Execution state
- Records Event History
- Processes Workflow state transitions

### Matching Service

- Manages Task Queues
- Matches Workflow and Activity Tasks with polling Workers

### Worker Service

- Runs internal Temporal system Workflows
- Does not execute application Workflow or Activity code

A production deployment may also include:

- A load balancer or ingress
- A supported persistence database
- Elasticsearch or another supported Visibility store, depending on deployment mode
- Monitoring and dashboards, such as Prometheus and Grafana

---

## 17. Polyglot Development

Temporal supports polyglot applications. Different parts of one application can use different SDK languages.

Example:

- A TypeScript Client starts a Workflow
- The Workflow is written in Go
- The Workflow invokes an Activity written in Java

This setup requires:

- A Go Worker to execute the Workflow code
- A Java Worker to execute the Activity code
- Compatible Task Queue and payload contracts

Run multiple instances of each Worker type across failure domains to improve availability and scalability.

---

## 18. Debugging Notes

### Worker Appears Active After Shutdown

After a Worker is stopped, the Web UI may still temporarily show recent poller information. It can display Workers that polled within the previous few minutes rather than only currently active processes.

If no compatible Worker is polling:

- Tasks remain in the Task Queue
- Schedule-to-Start latency increases
- The Task Queue backlog grows

### Debugging Checklist

1. Confirm that the Worker is connected to the correct Temporal address and Namespace.
2. Confirm that the Worker polls the expected Task Queue.
3. Confirm that the required Workflow and Activities are registered.
4. Check Worker logs for authentication, TLS, or gRPC failures.
5. Inspect the Workflow Event History in the Web UI.
6. Check pending tasks and Task Queue pollers.
7. Verify Activity timeout and Retry Policy settings.
8. Check for non-deterministic replay errors after deployments.
9. Confirm that payload codecs and Data Converters match between Clients and Workers.

---

## 19. Quick Reference

### When to Use Each Feature

| Requirement | Temporal feature |
|---|---|
| Execute non-deterministic or external work | Activity |
| Send an asynchronous state-changing message | Signal |
| Read Workflow state synchronously | Query |
| Signal a Workflow that might not exist | Signal-With-Start |
| Wait durably | Workflow Timer or `workflow.Sleep` |
| Detect progress in a long Activity | Heartbeat |
| Bound Activity execution time | Activity Timeout |
| Recover from transient Activity failures | Retry Policy |
| Avoid unbounded Event History | Continue-As-New |
| Search Workflow Executions | Visibility and Search Attributes |
| Protect payload contents | Custom Payload Codec |
| Safely evolve long-running Workflow code | Workflow versioning/deployment controls |

### Core Go SDK Flow

```go
// 1. Create a Temporal client.
c, err := client.Dial(client.Options{})
if err != nil {
    log.Fatal(err)
}
defer c.Close()

// 2. Create a Worker for a Task Queue.
w := worker.New(c, "my-task-queue", worker.Options{})

// 3. Register Workflow and Activity definitions.
w.RegisterWorkflow(MyWorkflow)
w.RegisterActivity(MyActivity)

// 4. Start polling and execute tasks.
err = w.Run(worker.InterruptCh())
if err != nil {
    log.Fatal(err)
}
```

---

## Suggested Repository Description

> Structured learning notes for Temporal, covering Workflows, Activities, Workers, Task Queues, Signals, Queries, Visibility, retries, timeouts, Event History, determinism, security, and cluster architecture, with Go SDK examples.

## Disclaimer

These notes were reorganized from personal study material. Temporal APIs, defaults, limits, and recommended deployment practices can change. Validate production decisions against the documentation for the Temporal Server, Temporal Cloud, CLI, and SDK versions in use.
