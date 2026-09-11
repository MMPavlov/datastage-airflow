"""Imports a generated DAG file with the Airflow stand-in and prints its structure, or a simulated run, as JSON.

usage: python run_dag.py <dags folder> <dag file> [--run] [--param NAME=VALUE ...]
"""

import argparse
import importlib.util
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("dags")
    parser.add_argument("dag_file")
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--param", action="append", default=[])
    args = parser.parse_args()

    sys.path.insert(0, os.path.join(HERE, "airflow_stub"))
    sys.path.insert(1, os.path.abspath(args.dags))
    from airflow import _stub

    spec = importlib.util.spec_from_file_location("dag_under_test", args.dag_file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    dags = [value for value in vars(module).values() if isinstance(value, _stub.DAG)]
    if len(dags) != 1:
        raise SystemExit("expected one DAG in %s, found %d" % (args.dag_file, len(dags)))
    dag = dags[0]

    output = {
        "dag_id": dag.dag_id,
        "params": sorted(dag.params),
        "tasks": [
            {"task_id": t.task_id, "kind": t.kind, "trigger_rule": t.trigger_rule,
             "upstream": sorted(u.task_id for u in t.upstream)}
            for t in dag.order
        ],
    }
    if args.run:
        params = dict(p.split("=", 1) for p in args.param)
        output.update(_stub.run_dag(dag, params))
        output["emails"] = _stub.EMAILS
        output["triggered"] = _stub.TRIGGERED
    json.dump(output, sys.stdout, indent=2, default=str)


if __name__ == "__main__":
    main()
