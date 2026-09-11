"""A small stand-in for Apache Airflow used by the ds2af tests (Airflow does not run natively on Windows).

It imports generated DAG files through the Airflow 2 and Airflow 3 import paths and simulates a DAG run
with Airflow's rules: trigger rules, skip propagation, branch tasks skipping the direct downstream tasks
they do not select, sensors and XCom. It covers what ds2af generates, nothing more.
"""

import datetime
import json
import traceback

from airflow.exceptions import AirflowSensorTimeout, AirflowSkipException

EMAILS = []
TRIGGERED = []
_CURRENT = []


class DAG:
    def __init__(self, dag_id, **kwargs):
        self.dag_id = dag_id
        self.kwargs = kwargs
        self.params = kwargs.get("params") or {}
        self.tasks = {}
        self.order = []

    def __enter__(self):
        _CURRENT.append(self)
        return self

    def __exit__(self, *exc):
        _CURRENT.pop()
        return False

    def add(self, task):
        if task.task_id in self.tasks:
            raise ValueError("duplicate task id %s in DAG %s" % (task.task_id, self.dag_id))
        self.tasks[task.task_id] = task
        self.order.append(task)


def _flatten(items):
    if isinstance(items, (list, tuple)):
        for item in items:
            yield from _flatten(item)
    else:
        yield items


class BaseOperator:
    kind = "operator"

    def __init__(self, task_id, trigger_rule="all_success", **kwargs):
        if not _CURRENT:
            raise RuntimeError("operator %s created outside a DAG" % task_id)
        self.task_id = task_id
        self.trigger_rule = str(trigger_rule)
        self.kwargs = kwargs
        self.upstream = []
        self.downstream = []
        self.dag = _CURRENT[-1]
        self.dag.add(self)

    def set_downstream(self, other):
        for task in _flatten(other):
            if task not in self.downstream:
                self.downstream.append(task)
                task.upstream.append(self)

    def __rshift__(self, other):
        self.set_downstream(other)
        return other

    def __rrshift__(self, other):
        for task in _flatten(other):
            task.set_downstream(self)
        return self

    def execute(self, context):
        return None


class EmptyOperator(BaseOperator):
    kind = "empty"


class TriggerDagRunOperator(BaseOperator):
    kind = "trigger_dagrun"

    def __init__(self, task_id, trigger_dag_id, conf=None, **kwargs):
        super().__init__(task_id, **kwargs)
        self.trigger_dag_id = trigger_dag_id
        self.conf = conf
        if isinstance(conf, BaseOperator):
            conf.set_downstream(self)

    def execute(self, context):
        conf = self.conf
        if isinstance(conf, BaseOperator):
            conf = context["ti"].xcom_pull(task_ids=conf.task_id)
        TRIGGERED.append({"dag_id": self.trigger_dag_id, "conf": conf})


class PythonTask(BaseOperator):
    def __init__(self, function, kind, task_id=None, **kwargs):
        super().__init__(task_id or function.__name__, **kwargs)
        self.function = function
        self.kind = kind

    def execute(self, context):
        return self.function(**context)


class _Decorator:
    def __init__(self, kind):
        self.kind = kind

    def __call__(self, python_callable=None, **kwargs):
        if callable(python_callable):
            return self._wrap(python_callable, {})

        def decorator(function):
            return self._wrap(function, kwargs)

        return decorator

    def _wrap(self, function, kwargs):
        kind = self.kind

        def factory(*args, **call_kwargs):
            return PythonTask(function, kind, **kwargs)

        factory.__name__ = function.__name__
        return factory


class _TaskDecorators(_Decorator):
    def __init__(self):
        super().__init__("task")
        self.branch = _Decorator("branch")
        self.sensor = _Decorator("sensor")


task = _TaskDecorators()


class Param:
    def __init__(self, default=None, **schema):
        self.default = default
        self.schema = schema


class _TaskInstance:
    def __init__(self, task_id, store):
        self.task_id = task_id
        self._store = store

    def xcom_push(self, key, value):
        self._store[(self.task_id, key)] = json.loads(json.dumps(value))

    def xcom_pull(self, task_ids=None, key="return_value"):
        if task_ids is None:
            task_ids = self.task_id
        if isinstance(task_ids, str):
            return self._store.get((task_ids, key))
        return [self._store.get((t, key)) for t in task_ids]


def _decide(rule, states):
    n = len(states)
    if n == 0:
        return "run"
    success = states.count("success")
    failed = states.count("failed") + states.count("upstream_failed")
    skipped = states.count("skipped")
    if rule == "all_success":
        return "run" if success == n else ("upstream_failed" if failed else "skipped")
    if rule == "all_failed":
        return "run" if failed == n else "skipped"
    if rule == "all_done":
        return "run"
    if rule == "one_success":
        return "run" if success else ("skipped" if skipped == n else "upstream_failed")
    if rule == "one_failed":
        return "run" if failed else "skipped"
    if rule == "none_failed":
        return "upstream_failed" if failed else "run"
    if rule == "none_failed_min_one_success":
        return "upstream_failed" if failed else ("run" if success else "skipped")
    if rule == "none_skipped":
        return "skipped" if skipped else "run"
    raise ValueError("trigger rule %s is not simulated" % rule)


def _descendants(start):
    seen = set()
    stack = list(start.downstream)
    while stack:
        current = stack.pop()
        if current.task_id not in seen:
            seen.add(current.task_id)
            stack.extend(current.downstream)
    return seen


def _toposort(dag):
    order, done, visiting = [], set(), set()

    def visit(t):
        if t.task_id in done:
            return
        if t.task_id in visiting:
            raise ValueError("cycle at task %s" % t.task_id)
        visiting.add(t.task_id)
        for upstream in t.upstream:
            visit(upstream)
        visiting.discard(t.task_id)
        done.add(t.task_id)
        order.append(t)

    for t in dag.order:
        visit(t)
    return order


def run_dag(dag, params=None, logical_date=None):
    """Runs each task once in dependency order, the way the Airflow scheduler would decide it."""
    store, states, errors, values = {}, {}, {}, {}
    for name, value in (dag.params or {}).items():
        values[name] = value.default if isinstance(value, Param) else value
    values.update(params or {})
    date = logical_date or datetime.datetime(2005, 6, 15, tzinfo=datetime.timezone.utc)
    for t in _toposort(dag):
        if t.task_id in states:
            continue
        decision = _decide(t.trigger_rule, [states[u.task_id] for u in t.upstream])
        if decision != "run":
            states[t.task_id] = decision
            continue
        context = {"ti": _TaskInstance(t.task_id, store), "params": dict(values), "logical_date": date,
                   "dag": dag, "task": t}
        try:
            result = t.execute(context)
            if t.kind == "sensor" and not result:
                raise AirflowSensorTimeout("sensor %s timed out" % t.task_id)
            if t.kind == "branch":
                chosen = {result} if isinstance(result, str) else set(result or [])
                keep = set(chosen)
                for task_id in chosen:
                    if task_id not in dag.tasks:
                        raise ValueError("branch %s returned unknown task id %s" % (t.task_id, task_id))
                    keep |= _descendants(dag.tasks[task_id])
                for downstream in t.downstream:
                    if downstream.task_id not in keep and downstream.task_id not in states:
                        states[downstream.task_id] = "skipped"
            if result is not None:
                store[(t.task_id, "return_value")] = json.loads(json.dumps(result))
            states[t.task_id] = "success"
        except AirflowSkipException as exc:
            states[t.task_id] = "skipped"
            errors[t.task_id] = "skipped: %s" % exc
        except Exception as exc:  # noqa: BLE001 - a failing task must not stop the simulation
            states[t.task_id] = "failed"
            errors[t.task_id] = "%s: %s\n%s" % (type(exc).__name__, exc, traceback.format_exc())
    return {"states": states, "errors": errors, "xcom": {"%s|%s" % key: value for key, value in store.items()}}
