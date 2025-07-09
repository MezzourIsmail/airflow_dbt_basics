import pytest
from airflow.models import DagBag

dag_bag = DagBag(include_examples=False)

def test_all_dags_have_tags():
    """
    Ensure every DAG in the DagBag has at least one tag
    """
    missing = []
    for dag_id, dag in dag_bag.dags.items():
        if not getattr(dag, "tags", None):
            missing.append(dag_id)

    assert not missing, f"The following DAGs are missing tags: {', '.join(missing)}"
