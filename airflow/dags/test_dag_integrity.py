import unittest
from airflow.models import DagBag

class TestDagBag(unittest.TestCase):

    def setUp(self):
        self.dagbag = DagBag()

    def test_dag_import(self):
        self.assertFalse(
            len(self.dagbag.import_errors),
            f"DAG import failures: {self.dagbag.import_errors}"
        )

    def test_test_dag_loaded(self):
        self.assertIn("test_dag", self.dagbag.dags)
        dag = self.dagbag.get_dag("test_dag")
        self.assertIsNotNone(dag)
        self.assertEqual(len(dag.tasks), 1)
        self.assertIn("start", [t.task_id for t in dag.tasks])
