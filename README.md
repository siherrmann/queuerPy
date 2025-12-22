# queuerPy

[![PyPi](https://pythonico.leapcell.app/pypi/queuerPy.svg?style=shields&data=n,v,d)](https://pypi.org/project/queuerPy/)
[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
![Coverage](https://raw.githubusercontent.com/siherrmann/queuerPy/refs/heads/main/coverage-badge.svg)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/siherrmann/queuer/blob/master/LICENSE)

Python port of the queuer package - a queueing system based on PostgreSQL.

## 💡 Goal of this package

This queuer is meant to be as easy as possible to use. No specific function signature (except for returning results or raising exceptions for error handling), easy setup and still fast.

The job table contains only queued, scheduled and running tasks. The ended jobs (succeeded, cancelled, failed) are moved to a job_archive table.

---

# 🛠️ Installation

## PyPI Installation (Recommended)

```bash
pip install queuerPy
```

To use the package you also need a running postgres database with the timescaleDB extension. You can use the `docker-compose.yml` file in the example folder or start a Docker container with the `timescale/timescaledb:latest-pg17` image.

---

## 🚀 Getting started

The full initialisation is (in the easiest case):

```python
from queuer import new_queuer

# Create a new queuer instance
q = new_queuer("exampleWorker", 3)

# Add a task to the queuer
q.add_task(example_task)

# Start the queuer
q.start()
```

You can also add a task with a decorator so the tasks don't have to be added to some central function:

```python
@queuer.task(name="optionalName")
def example_task():
...
```

That's easy, right? Adding a job is just as easy:

```python
# Add a job to the queue with positional arguments
job = q.add_job(example_task, 5, "12")
print(f"Job added: {job.rid}")

# Add a job with keyword arguments (kwargs)
job = q.add_job(example_task, paramKeyed1="test", debug=True)

# Add a job with both positional and keyword arguments
job = q.add_job(example_task, 5, "12", paramKeyed1="test", debug=True)
```

In the initialisation of the queuer the existence of the necessary database tables is checked and if they don't exist they get created. The database is configured with these environment variables:

```bash
export QUEUER_DB_HOST=localhost
export QUEUER_DB_PORT=5432
export QUEUER_DB_DATABASE=postgres
export QUEUER_DB_USERNAME=username
export QUEUER_DB_PASSWORD=password1234
export QUEUER_DB_SCHEMA=public
```

You can find a full example in the example folder.

---

## new_queuer

`new_queuer` is a convenience constructor that creates a new Queuer instance using default database configuration derived from environment variables. It acts as a wrapper around `new_queuer_with_db`. The encryption key for the database is taken from the `QUEUER_ENCRYPTION_KEY` environment variable; if not provided, it defaults to unencrypted results.

`new_queuer_with_db` is the primary constructor for creating a new Queuer instance. It allows for explicit database configuration and encryption key specification, and initializes all necessary components, including database handlers, internal event listeners, and the worker.

```python
def new_queuer(name: str, max_concurrency: int, *options: OnError) -> Queuer

def new_queuer_with_db(
    name: str, 
    max_concurrency: int, 
    encryption_key: str, 
    db_config: DatabaseConfiguration, 
    *options: OnError
) -> Queuer
```

- `name`: A `str` identifier for this queuer instance.
- `max_concurrency`: An `int` specifying the maximum number of jobs this queuer can process concurrently.
- `encryption_key`: A `str` used for encrypting sensitive job data in the database. If empty, results will be stored unencrypted.
- `db_config`: An optional `DatabaseConfiguration`. If None, the configuration will be loaded from environment variables.
- `options`: Optional `OnError` configurations to apply to the worker.

This function performs the following setup:
- Initializes a logger.
- Sets up the database connection using the provided `db_config` or environment variables.
- Creates `JobDBHandler`, `WorkerDBHandler` instances for database interactions.
- Initializes internal notification listeners for `job_insert`, `job_update`, and `job_delete` events.
- Creates and inserts a new `Worker` into the database based on the provided `name`, `max_concurrency`, and `options`.
- If any critical error occurs during this initialization (e.g., database connection failure, worker creation error), the function will raise an exception.

---

## start

The `start` method initiates the operational lifecycle of the Queuer. It sets up the main processing loops, initializes database listeners, and begins the job processing and polling loops.

```python
def start(self) -> None
```

Upon calling `start`:
- It performs a basic check to ensure internal listeners are initialized.
- Database listeners are created to listen to job events (inserts, updates, deletes) via PostgreSQL NOTIFY/LISTEN.
- It starts a poller to periodically poll the database for new jobs to process.
- It starts a heartbeat ticker to keep the worker status updated.
- The method returns immediately after starting all background processes.

The method includes proper error handling and will raise exceptions if the queuer is not properly initialized or if there's an error creating the database listeners.

---

## stop

The `stop` method gracefully shuts down the Queuer instance, releasing resources and ensuring ongoing operations are properly concluded.

```python
def stop(self) -> None
```

The `stop` method cancels all jobs, closes database listeners, and cleans up resources.

---

## add_task

The `add_task` method registers a new job task with the queuer. A task is the actual function that will be executed when a job associated with it is processed.

```python
def add_task(self, task: Callable) -> Task

def add_task_with_name(self, task: Callable, name: str) -> Task
```

- `task`: A `Callable` representing the function that will serve as the job's executable logic. The queuer will automatically derive a name for this task based on its function name (e.g., `my_task_function`). The derived name must be unique if no `name` is given.
- `name`: A `str` specifying the custom name for this task. This name must be unique within the queuer's tasks.

This method handles the registration of a task, making the worker able to pick up and execute a job of this task type. It also updates the worker's available tasks in the database. The task should be added before starting the queuer. If there's an issue during task creation or database update, an exception will be raised.

---

## add_job

The `add_job` method adds a new job to the queue for execution. Jobs are units of work that will be processed by the queuer.

```python
def add_job(
    self, 
    task: Union[Callable, str], 
    *parameters: Any,
    **parameters_keyed: Any
) -> Job

def add_job_with_options(
    self,
    options: Optional[Options],
    task: Union[Callable, str],
    *parameters: Any,
    **parameters_keyed: Any
) -> Job
```

- `task`: A `Callable` or `str` representing the task to execute. If a callable, it must be registered with `add_task` first.
- `options`: Optional `Options` for custom error handling or scheduling behavior. Only available in `add_job_with_options()`.
- `*parameters`: Positional arguments to pass to the task function.
- `**parameters_keyed`: Keyword arguments to pass to the task function (Python-specific feature).

**Examples:**

```python
# Job with both positional and keyword arguments
job = queuer.add_job(my_task, "arg1", paramKeyed1="test", paramKeyed2=1)

# Job with custom options (requires add_job_with_options)
options = Options(on_error=OnError(max_retries=5))
job = queuer.add_job_with_options(options, my_task, "arg1", debug=True)
```

**Note:** Keyword arguments (`**parameters_keyed`) are stored separately in the database and enable Python functions to be called with named parameters. This is useful for optional parameters, default values, and improved code clarity. Go jobs continue to use only positional parameters.

---

## add_jobs

The `add_jobs` method allows efficient batch insertion of multiple jobs at once.

```python
def add_jobs(self, batch_jobs: List[BatchJob]) -> List[UUID]
```

- `batch_jobs`: A list of `BatchJob` instances to insert.

**Example:**

```python
from model.batch_job import BatchJob

batch = [
    BatchJob(task=my_task, parameters=[1, "a"]),
    BatchJob(task=my_task, parameters=[2], parameters_keyed={"name": "b"}),
    BatchJob(task=my_task, parameters_keyed={"id": 3, "name": "c"})
]
job_rids = queuer.add_jobs(batch)
```

---

## add_next_interval_func

The `add_next_interval_func` method registers a custom function that determines the next execution time for scheduled jobs. This is useful for implementing complex scheduling logic beyond simple fixed intervals.

```python
def add_next_interval_func(self, nif: Callable) -> Worker

def add_next_interval_func_with_name(self, nif: Callable, name: str) -> Worker
```

- `nif`: A `Callable` defining custom logic for calculating the next interval. The queuer will automatically derive a name for this function. The derived name must be unique if no `name` is given.
- `name`: A `str` specifying the custom name for this NextIntervalFunc. This name must be unique within the queuer's NextIntervalFuncs.

This method adds the provided NextIntervalFunc to the queuer's available functions, making it usable for jobs with custom scheduling requirements. It updates the worker's configuration in the database.

---

## Worker Options

The OnError class defines how a worker should handle errors when processing a job. This allows for configurable retry behavior.

```python
class OnError:
    def __init__(
        self,
        timeout: float = 30.0,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        retry_backoff: str = RetryBackoff.NONE
    )
```

- `timeout`: The maximum time (in seconds) allowed for a single attempt of a job. If the job exceeds this duration, it's considered to have timed out.
- `max_retries`: The maximum number of times a job will be retried after a failure.
- `retry_delay`: The initial delay (in seconds) before the first retry attempt. This delay can be modified by the `retry_backoff` strategy.
- `retry_backoff`: Specifies the strategy used to increase the delay between subsequent retries.

#### Retry Backoff Strategies

The RetryBackoff enum defines the available strategies for increasing retry delays:

```python
class RetryBackoff(str, Enum):
    NONE = "none"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
```

- `RETRY_BACKOFF_NONE`: No backoff. The retry_delay remains constant for all retries.
- `RETRY_BACKOFF_LINEAR`: The retry delay increases linearly with each attempt (e.g., delay, 2*delay, 3*delay).
- `RETRY_BACKOFF_EXPONENTIAL`: The retry delay increases exponentially with each attempt (e.g., delay, delay*2, delay*2*2).

---

## Job options

The Options class allows you to define specific behaviors for individual jobs, overriding default worker settings where applicable.

```python
@dataclass
class Options:
    on_error: Optional[OnError] = None
    schedule: Optional[Schedule] = None
```

- `on_error`: An optional `OnError` configuration that will override the worker's default error handling for this specific job. This allows you to define unique retry logic per job.
- `schedule`: An optional `Schedule` configuration for jobs that need to be executed at recurring intervals.

### OnError for jobs

The OnError class for jobs is identical to the one used for worker options, allowing granular control over error handling for individual jobs.

### Schedule

The Schedule class is used to define recurring jobs.

```python
@dataclass
class Schedule:
    start: datetime = None
    max_count: int = 1
    interval: Optional[timedelta] = None
    next_interval: Optional[str] = None
```

- `start`: The initial time at which the scheduled job should first run.
- `max_count`: The maximum number of times the job should be executed. A value of 0 indicates an indefinite number of repetitions (run forever).
- `interval`: The duration between consecutive executions of the scheduled job.
- `next_interval`: Function name of the NextIntervalFunc returning the time of the next execution of the scheduled job. **Either `interval` or `next_interval` have to be set if the `max_count` is 0 or greater than 1.**

---

## Additional Methods

### Job Management

```python
# Add a job with both positional and keyword arguments
job = queuer.add_job(my_task, param1, param2, paramKeyed1="test", paramKeyed2=1)

# Add a job with custom options
from model.options import Options, OnError
options = Options(on_error=OnError(max_retries=5, timeout=60.0))
job = queuer.add_job_with_options(options, my_task, param1, paramKeyed1="test")

# Add multiple jobs as a batch
from model.batch_job import BatchJob
batch = [
    BatchJob(task=my_task, parameters=[1, "a"]),
    BatchJob(task=my_task, parameters=[2], parameters_keyed={"name": "b"}),
    BatchJob(task=my_task, parameters_keyed={"id": 3, "name": "c"})
]
queuer.add_jobs(batch)

# Wait for a job to finish
finished_job = queuer.wait_for_job_finished(job.rid, timeout_seconds=30.0)

# Get job information
job_info = queuer.get_job(job.rid)
archived_job = queuer.get_job_ended(job.rid)  # For completed jobs
```

### Job Queries

```python
# Get jobs by status
running_jobs = queuer.get_jobs(status="RUNNING")
all_jobs = queuer.get_jobs()

# Get jobs by worker
worker_jobs = queuer.get_jobs_by_worker_rid(worker.rid)
```

---

# ⭐ Features

- **Keyword Arguments Support**: Python functions can use both positional arguments (`*args`) and keyword arguments (`**kwargs`), while maintaining compatibility with Go jobs that use only positional parameters.
- **Async/Await Support**: Full asyncio integration with threading fallbacks.
- **PostgreSQL NOTIFY/LISTEN**: Real-time job notifications without polling overhead.
- **Batch Job Processing**: Insert job batches efficiently using PostgreSQL's `COPY FROM` feature.
- **Panic Recovery**: Automatic recovery for all running jobs in case of unexpected failures.
- **Error Handling**: Comprehensive error handling by checking last output parameter for errors.
- **Multiple Workers**: Multiple queuer instances can run across different microservices while maintaining job start order and isolation.
- **Scheduled Jobs**: Support for scheduled and periodic jobs with custom intervals.
- **Job Lifecycle Management**: Easy functions to get jobs and workers, track job status.
- **Event Listeners**: Listen for job updates, completion, and deletion events (ended jobs).
- **Job Completion Helpers**: Helper functions to listen for specific finished jobs.
- **Retry Mechanisms**: Retry mechanism for ended jobs which creates a new job with the same parameters, with configurable retry logic and different backoff strategies.
- **Custom Scheduling**: Custom NextInterval functions to address custom needs for scheduling (e.g., scheduling with timezone offset).
- **Master Worker Management**: Automatic master worker setting retention and other central settings. Automatic switch to new master if old worker stops.
- **Heartbeat System**: Worker heartbeat monitoring and automatic stale worker detection and cancellation by the master.
- **Encryption Support**: Encryption support for sensitive job data stored in the database.
- **Database Integration**: Seamless PostgreSQL integration with automatic schema management.
- **Type Safety**: Full type hints and dataclass-based models for better development experience.

> **Note**: Transactional job insert is the only feature that is not yet implemented in the Python implementation of the queuer.

---

# 🧪 Testing

```bash
# Run all tests
python -m pytest

# Run with coverage
python -m pytest --cov=. --cov-report=html

# Run specific test files
python -m pytest queuer_test.py -v
```
