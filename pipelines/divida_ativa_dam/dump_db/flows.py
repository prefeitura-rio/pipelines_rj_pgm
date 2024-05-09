# -*- coding: utf-8 -*-
"""
Database dumping flows for DAM divida_ativa PGM
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_templates.dump_db.flows import flow as dump_sql_flow
from prefeitura_rio.pipelines_utils.prefect import set_default_parameters
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

from pipelines.constants import constants
from pipelines.divida_ativa_dam.dump_db.schedules import (
    divida_ativa_daily_update_schedule,
)

rj_pgm_dump_db_divida_ativa_flow = deepcopy(dump_sql_flow)
rj_pgm_dump_db_divida_ativa_flow.state_handlers = [
    handler_inject_bd_credentials,
    handler_initialize_sentry,
]
rj_pgm_dump_db_divida_ativa_flow.name = "PGM: DAM - divida ativa - Ingerir tabelas de banco SQL"
rj_pgm_dump_db_divida_ativa_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)

rj_pgm_dump_db_divida_ativa_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_PGM_AGENT_LABEL.value,  # label do agente
    ],
)

rj_pgm_dump_db_divida_ativa_default_parameters = {
    "db_database": "DAM_PRD",
    "db_host": "10.2.221.127",
    "db_port": "1433",
    "db_type": "sql_server",
    "dataset_id": "adm_financas_divida_ativa",
    "infisical_secret_path": "/db-dam-prod",
}

rj_pgm_dump_db_divida_ativa_flow = set_default_parameters(
    rj_pgm_dump_db_divida_ativa_flow,
    default_parameters=rj_pgm_dump_db_divida_ativa_default_parameters,
)

rj_pgm_dump_db_divida_ativa_flow.schedule = divida_ativa_daily_update_schedule
