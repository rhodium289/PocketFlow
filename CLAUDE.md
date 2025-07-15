# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PocketFlow is a minimalist LLM framework that implements complex AI workflows in just 100 lines of core code. It follows the "Agentic Coding" philosophy where humans design the architecture and AI agents implement the code.

## Core Architecture

### Framework Design (pocketflow/__init__.py)
The entire framework is contained in a single 100-line file with these core abstractions:

- **BaseNode** - Foundation for all nodes with successor management
- **Node** - Basic execution unit with retry/fallback mechanisms
- **BatchNode** - Processes lists of items in sequence
- **Flow** - Orchestrates node execution with action-based transitions
- **BatchFlow** - Runs flows multiple times with different parameters
- **AsyncNode/AsyncFlow** - Async versions for I/O-bound operations
- **AsyncParallelBatchNode/AsyncParallelBatchFlow** - Parallel execution versions

### Node Execution Pattern
Every node follows a 3-step pattern:
1. **prep(shared)** - Read/preprocess data from shared store
2. **exec(prep_res)** - Execute core logic (LLM calls, computations)
3. **post(shared, prep_res, exec_res)** - Write results back, return action

### Communication Pattern
- **Shared Store** - Global dictionary for inter-node communication
- **Action-based Transitions** - Nodes return actions to determine next steps
- **Graph-based Workflow** - Nodes connected by action mappings

## Development Commands

```bash
# Install package for development
pip install -e .

# Install with Redis support
pip install -e .[redis]

# Run tests
python -m pytest tests/

# Run individual cookbook examples
cd cookbook/pocketflow-hello-world/
python main.py

# Install example dependencies
cd cookbook/example-name/
pip install -r requirements.txt

# Run Redis-based distributed example
cd cookbook/pocketflow-redis-example/
redis-server &  # Start Redis server
python worker.py --worker-id 1 &  # Start worker
python job_creator.py --num-jobs 5 --monitor  # Create jobs
```

## Project Structure

```
pocketflow/               # Core framework (100 lines)
├── __init__.py          # Main framework code
└── __init__.pyi         # Type hints

cookbook/                # 40+ example applications
├── pocketflow-hello-world/
├── pocketflow-agent/
├── pocketflow-rag/
├── pocketflow-multi-agent/
└── ...

tests/                   # Unit tests using unittest
docs/                    # Jekyll documentation
utils/                   # Utility scripts
```

## Key Design Patterns Supported

### AI Patterns
- **Agents** - Dynamic action selection based on context
- **Workflows** - Multi-step task decomposition
- **RAG** - Retrieval-augmented generation
- **Map-Reduce** - Large-scale data processing
- **Multi-Agent Systems** - Agent coordination and communication

### Implementation Patterns
- **Graph-based Workflow** - Action-based transitions between nodes
- **Shared Store Communication** - Global data structure
- **Retry and Fallback** - Built-in error handling
- **Batch Processing** - Efficient handling of large datasets
- **Async/Parallel Execution** - I/O-bound task optimization

## Agentic Coding Guidelines (from .cursorrules)

### Code Organization
- Keep the core framework minimal (100 lines)
- Each cookbook example should be self-contained
- Use type hints consistently (see __init__.pyi)
- Follow the 3-step node execution pattern

### Testing Approach
- Unit tests in tests/ directory using unittest
- Test both sync and async versions of components
- Focus on flow execution, node behavior, and error handling
- Each cookbook example should include usage instructions

### Development Philosophy
- **Separation of Concerns** - Data, compute, and orchestration separated
- **Fail Fast** - Built-in retry mechanisms for reliability
- **Zero Vendor Lock-in** - No built-in API dependencies
- **Composable Components** - Reusable, extensible building blocks

## Common Development Tasks

### Adding New Node Types
Extend BaseNode and implement prep/exec/post methods following the established pattern.

### Creating New Examples
- Create new directory in cookbook/
- Include requirements.txt for dependencies
- Follow existing example structure
- Add clear documentation in main.py

### Modifying Core Framework
- Maintain the 100-line constraint
- Update type hints in __init__.pyi
- Ensure backward compatibility
- Add comprehensive tests

## Documentation Structure

The docs/ directory contains Jekyll-based documentation covering:
- Core abstractions and their usage
- Design patterns and implementation guides
- Agentic coding methodology
- API reference and examples

## Redis-Based Distributed Execution

### Optional Redis Backend
PocketFlow supports Redis-based distributed execution with job_id isolation:

```python
from pocketflow import RedisSharedStore, RedisQueueManager, RedisWorker

# Job-isolated shared store
shared = RedisSharedStore(job_id="abc123")
shared["data"] = "isolated per job"

# Queue manager for job distribution
queue_manager = RedisQueueManager()
job_id = queue_manager.create_job("ProcessNode", {"param": "value"})

# Distributed worker processing
worker = RedisWorker(node_registry, queue_manager)
worker.run()  # Processes jobs from queue
```

### Redis Components
- **RedisSharedStore/AsyncRedisSharedStore** - Job-isolated shared memory
- **RedisQueueManager/AsyncRedisQueueManager** - Job queue and result storage
- **RedisWorker/AsyncRedisWorker** - Distributed job processors
- **QueueMessage** - Structured job messages with job_id context

### Installation
```bash
pip install pocketflow[redis]
```

### Architecture Benefits
- **Scalability** - Add workers across processes/machines
- **Fault Tolerance** - Jobs persist in Redis if workers fail
- **Isolation** - Complete data isolation per job_id
- **Monitoring** - Track job status and worker performance

## Key Constraints and Principles

- **Minimalism** - Core framework must remain under 100 lines
- **Zero Dependencies** - Framework itself has no external dependencies
- **Type Safety** - Comprehensive type hints provided
- **Extensibility** - Easy to add new patterns and node types
- **Reliability** - Built-in error handling and retry mechanisms