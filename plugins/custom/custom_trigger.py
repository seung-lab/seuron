from airflow.plugins_manager import AirflowPlugin
from datetime import datetime
import logging
import types
import collections

from airflow.models import BaseOperator
from airflow.models import DagBag
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State
from airflow import settings


class MultiTriggerDagRunOperator(BaseOperator):
    """
    Triggers multiple DAG runs for a specified ``dag_id``.

    Draws inspiration from:
        airflow.operators.dagrun_operator.TriggerDagRunOperator

    :param trigger_dag_id: the dag_id to trigger
    :type trigger_dag_id: str
    :param params_list: list of dicts or a thunked generator for DAG level parameters that are made acesssible
        in templates namespaced under params for each dag run.
    :type params: Iterable<dict> or types.GeneratorType
    """

    @apply_defaults
    def __init__(
            self,
            trigger_dag_id,
            params_list,
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.trigger_dag_id = trigger_dag_id
        self.params_list = params_list

        if hasattr(self.params_list, '__len__'):
            assert len(self.params_list) > 0
        else:
            if callable(params_list):
                generator = params_list()
            else:
                generator = params_list
            assert (isinstance(generator, collections.Iterable) or isinstance(generator, types.GeneratorType)), \
                ('Params list returned %s, must return either an iterable, generator, or callable to returns an ' +
                 'iterable or generator') % type(generator)

    def execute(self, context):
        session = settings.Session()
        dbag = DagBag(settings.DAGS_FOLDER)
        trigger_dag = dbag.get_dag(self.trigger_dag_id)

        assert trigger_dag is not None

        trigger_id = 0
        for params in self.params_list if not callable(self.params_list) else self.params_list():
            dr = trigger_dag.create_dagrun(run_id='trig_%s_%d_%s' %
                                           (self.trigger_dag_id, trigger_id,
                                            datetime.now().isoformat()),
                                           state=State.RUNNING,
                                           conf=params,
                                           external_trigger=True)
            logging.info("Creating DagRun {}".format(dr))
            session.add(dr)
            trigger_id = trigger_id + 1
            if trigger_id % 10:
                session.commit()
        session.commit()
        session.close()


class CustomPlugin(AirflowPlugin):
    name = "custom_plugin"
    operators = [MultiTriggerDagRunOperator]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
