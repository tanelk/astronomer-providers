import asyncio

import pytest
from airflow.models import DagRun, TaskInstance
from airflow.operators.dummy import DummyOperator
from airflow.utils import timezone
from airflow.utils.state import DagRunState, TaskInstanceState

from astronomer.providers.core.triggers.external_task import (
    DagStateTrigger,
    TaskStateTrigger,
)


class TestTaskStateTrigger:
    DEFAULT_DATE = timezone.datetime(2015, 1, 1)
    DAG_ID = "unit_test_dag"
    TASK_ID = "external_task_sensor_check"
    RUN_ID = "unit_test_dag_run_id"
    STATES = ["success", "fail"]
    POLL_INTERVAL = 3.0

    @pytest.mark.asyncio
    async def test_task_state_trigger(self, session, dag):
        """
        Asserts that the TaskStateTrigger only goes off on or after a TaskInstance
        reaches an allowed state (i.e. SUCCESS).
        """
        dag_run = DagRun(dag.dag_id, run_type="manual", execution_date=self.DEFAULT_DATE, run_id=self.RUN_ID)

        session.add(dag_run)
        session.commit()

        external_task = DummyOperator(task_id=self.TASK_ID, dag=dag)
        instance = TaskInstance(external_task, self.DEFAULT_DATE)
        session.add(instance)
        session.commit()

        trigger = TaskStateTrigger(
            dag_id=dag.dag_id,
            task_id=instance.task_id,
            states=self.STATES,
            execution_dates=[self.DEFAULT_DATE],
            poll_interval=0.2,
        )

        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # It should not have produced a result
        assert task.done() is False

        # Progress the task to a "success" state so that run() yields a TriggerEvent
        instance.state = TaskInstanceState.SUCCESS
        session.commit()
        await asyncio.sleep(0.5)
        assert task.done() is True

        # Prevents error when task is destroyed while in "pending" state
        asyncio.get_event_loop().stop()

    def test_serialization(self):
        """
        Asserts that the TaskStateTrigger correctly serializes its arguments
        and classpath.
        """
        trigger = TaskStateTrigger(
            dag_id=self.DAG_ID,
            task_id=self.TASK_ID,
            states=self.STATES,
            execution_dates=[self.DEFAULT_DATE],
            poll_interval=self.POLL_INTERVAL,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "astronomer.providers.core.triggers.external_task.TaskStateTrigger"
        assert kwargs == {
            "dag_id": self.DAG_ID,
            "task_id": self.TASK_ID,
            "states": self.STATES,
            "execution_dates": [self.DEFAULT_DATE],
            "poll_interval": self.POLL_INTERVAL,
        }


class TestDagStateTrigger:
    DAG_ID = "test_dag"
    DAG_RUN_ID = "test_dag_run_id"
    STATES = ["success", "fail"]
    POLL_INTERVAL = 3.0
    DEFAULT_DATE = timezone.datetime(2015, 1, 1)

    @pytest.mark.asyncio
    async def test_dag_state_trigger(self, session, dag):
        """
        Assert that the DagStateTrigger only goes off on or after a DagRun
        reaches an allowed state (i.e. SUCCESS).
        """
        dag_run = DagRun(
            dag.dag_id, run_type="manual", execution_date=self.DEFAULT_DATE, run_id=self.DAG_RUN_ID
        )

        session.add(dag_run)
        session.commit()

        trigger = DagStateTrigger(
            dag_id=dag.dag_id,
            states=self.STATES,
            execution_dates=[self.DEFAULT_DATE],
            poll_interval=0.2,
        )

        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # It should not have produced a result
        assert task.done() is False

        # Progress the dag to a "success" state so that yields a TriggerEvent
        dag_run.state = DagRunState.SUCCESS
        session.commit()
        await asyncio.sleep(0.5)
        assert task.done() is True

        # Prevents error when task is destroyed while in "pending" state
        asyncio.get_event_loop().stop()

    def test_serialization(self):
        """Asserts that the DagStateTrigger correctly serializes its arguments and classpath."""
        trigger = DagStateTrigger(
            dag_id=self.DAG_ID,
            states=self.STATES,
            execution_dates=[self.DEFAULT_DATE],
            poll_interval=self.POLL_INTERVAL,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "astronomer.providers.core.triggers.external_dag.DagStateTrigger"
        assert kwargs == {
            "dag_id": self.DAG_ID,
            "states": self.STATES,
            "execution_dates": [self.DEFAULT_DATE],
            "poll_interval": self.POLL_INTERVAL,
        }
