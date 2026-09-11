# ds2af: DataStage to Airflow migrator

`ds2af` reads Ascential DataStage 7.5 exports (`.dsx` or XML; 8.x and 11.x DSX files work too) and
writes an Apache Airflow DAGs folder:

* every **job sequence** becomes a DAG. Activities become tasks, and triggers become dependencies,
  trigger rules and branch tasks;
* every **parallel or server job** whose stages and expressions all translate becomes a Python
  module that does the job's work (row-by-row transformers, hash lookups and joins, aggregations and so on);
* jobs that cannot be converted keep running in DataStage: their tasks call
  `dsjob -run -jobstatus`, locally or over SSH, and a partial conversion is saved as a draft;
* a **migration report** (Markdown and JSON) lists what was converted, how, and what needs a person.

The migrator is written in **C# 8**. `<LangVersion>8.0</LangVersion>` is set for the whole solution,
so the compiler rejects newer syntax. The engine targets `netstandard2.0` (usable from .NET Framework
4.6.1+ and .NET Core 3.x), and the command line tool targets `net8.0`.

## Build and test

```bash
dotnet build DataStage2Airflow.sln
```

```bash
dotnet test DataStage2Airflow.sln
```

The end-to-end tests run the generated Python. They need Python 3.9+ with pandas: `DS2AF_PYTHON`,
else `.venv` in the repository, else `python` on `PATH`. Airflow itself is not needed; the tests
import the DAG files with a small Airflow stand-in (`tests/DataStage2Airflow.Tests/Python/airflow_stub`)
that simulates a DAG run with Airflow's trigger rules, branch skipping and XCom.

## Usage

```bash
dotnet run --project src/DataStage2Airflow.Cli -- convert exports/ -o airflow_out
```

```bash
dotnet run --project src/DataStage2Airflow.Cli -- inspect exports/project.dsx
```

`inspect` runs the whole conversion in memory and prints the outcome per job and any blocking issues.

| Option | Meaning |
|---|---|
| `--mode auto` (default) | Python for jobs that translate completely, `dsjob` for the rest |
| `--mode python` | always generate Python; untranslatable parts raise at run time |
| `--mode dsjob` | orchestration only: every job keeps running in DataStage |
| `--airflow 2` / `--airflow 3` (default) | import paths of the generated DAGs |
| `--dsjob <path>`, `--dsjob-server <host>`, `--dsjob-ssh-conn <id>` | how DAG tasks reach DataStage |
| `--connection DSN=conn_id` | map a DataStage data source to an Airflow connection (repeatable) |
| `--schedule <cron>`, `--start-date`, `--owner`, `--dag-prefix` | DAG settings |
| `--warnings-ok` | OK triggers also fire when a job finishes with warnings |
| `--fail-on-warning` | a job that finishes with warnings fails its task |
| `--no-job-dags` | no DAGs for jobs that no sequence runs |
| `--strict` | exit code 3 when any issue blocks a conversion |

## Output

```
airflow_out/
  dags/
    <project>/seq_daily_load.py        one DAG per sequence, plus one per job no sequence runs
    ds2af_jobs/<project>/*.py          converted jobs; routines.py holds the server routines
    ds2af_runtime/                     runtime package used by DAGs and jobs
    .airflowignore                     keeps the two packages out of DAG parsing
  drafts/<project>/*.py                partial conversions of jobs left in DataStage
  reports/migration_report.md|json
```

Copy `dags/` into the Airflow DAGs folder and install pandas on the workers. Then create the Airflow
connections named in the report (database stages) and, for jobs still in DataStage, make `dsjob`
reachable. The Airflow Variable `ds2af_dsjob` (JSON, for example `{"ssh_conn_id": "ds_server"}`) overrides
the dsjob settings at run time without regenerating.

## How DataStage maps to Airflow

### Sequences

| DataStage | Airflow |
|---|---|
| Job activity | task running the converted job, or `dsjob`; a job that is another sequence becomes `TriggerDagRunOperator` |
| Routine activity | task calling `routines.<name>` |
| Execute Command | task running the command through the shell; `$ReturnValue` and `$CommandOutput` are kept |
| Notification | task sending mail through Airflow's e-mail setup |
| Wait-For-File | `@task.sensor` (reschedule mode) |
| Sequencer (all / any) | `EmptyOperator` with `all_success` / `one_success` |
| Nested Condition | `@task.branch` |
| Start Loop ... End Loop | one task that runs the loop body for each value |
| User Variables | task whose values later expressions read |
| Terminator | task that fails the run |
| Exception Handler | `EmptyOperator` with `one_failed`, downstream of the tasks it covers |
| OK / Unconditional triggers only | plain dependency (`all_success`, `none_skipped`) |
| Failed, Warning, Custom, UserStatus, ReturnValue, Otherwise | the activity becomes a `@task.branch` that runs it and returns the task ids of the triggers that fired |

A branch in Airflow skips the direct downstream tasks it does not select. A task that several
branches can trigger, such as a failure notification, is therefore reached through one small join task
per branch, and every branch keeps the exception handler selected. Trigger expressions
(`Job.$JobStatus = DSJS.RUNOK`, `$UserStatus`, `$ReturnValue`, `$Counter`, user variables, parameters,
macros) are translated to Python that reads the activity results from XCom.

### Jobs

Supported stages: Sequential File (parallel and server), Data Set, File Set, Lookup File Set,
Hashed File, Transformer (parallel and BASIC), Lookup, Join, Merge, Aggregator (parallel and server),
Sort (parallel and server plugin), Remove Duplicates, Funnel, Copy, Filter, Switch, Modify, Head, Tail,
Sample, Peek, Row Generator, Surrogate Key, Change Capture, Link Partitioner/Collector, IPC, and database
stages (Oracle, DB2, ODBC, Teradata, Informix, Sybase Enterprise stages and server plugins such as
ORAOCI9 and DRS) through Airflow connections.

Transformers keep their evaluation order: reference lookups, stage variables (which keep their values
between rows), then output links with their constraints, then the Otherwise link. A row that fails
evaluation goes to the reject link or is dropped with a warning, as the parallel engine does. About 140
transformer and BASIC functions are implemented in `ds2af_runtime/dsfunc.py`, including `Iconv`/`Oconv`
date, time and decimal codes and the `%yyyy-%mm-%dd` formats. Server jobs keep BASIC semantics: values
are strings, and numeric strings compare and calculate as numbers.

## Known differences and limits

* **Decimals** are written to text files zero-padded to their precision (`00000110.55`), as DataStage
  does. Use `DecimalToString(x, "suppress_zero")` in the job if that is unwanted.
* **OK triggers** do not fire when a job finishes with warnings (DataStage behaviour); the DAG skips the
  downstream tasks. `--warnings-ok` changes this.
* **Row order**: a continuous funnel concatenates its inputs in link order; hash joins keep the order of
  the left input.
* **Remove Duplicates** removes adjacent duplicates only, like DataStage, so the input must be sorted.
* **Surrogate keys** restart at the start value on each run; there is no key state file.
* **Loops** run inside one task, so their body activities do not appear as separate tasks.
* **Not converted**: local and shared containers, runtime column propagation, sparse lookups
  (`ORCHESTRATE.` placeholders in queries), batch jobs (job control BASIC), multi-statement server
  routines (stubs keep the BASIC source), `$ErrSource`/`$ErrMessage` in exception handlers. These are
  reported; in auto mode such jobs run through `dsjob`.
* **Sequence property names**: public DataStage documentation does not describe the export layout of
  sequence activities and triggers. The reader tries the known name variants, derives trigger types
  from their descriptions ("Executed OK") and expressions, and lists in the report every property it did
  not interpret and every trigger whose type rests on its numeric code alone. Check these against the
  Designer before going live.

## Project layout

```
src/DataStage2Airflow.Core      reader (Dsx/), model (Model/), expression engine (Expressions/),
                                generators (Generation/), report (Reporting/), Python runtime (Runtime/python)
src/DataStage2Airflow.Cli       ds2af command line
tests/DataStage2Airflow.Tests   xUnit tests; Python/ holds the Airflow stand-in and run_dag.py
tests/python                    unittest suite for the runtime functions
samples/dsx, samples/data       a 7.5.1 project export (7 jobs, 2 routines) with input and expected output data
```
