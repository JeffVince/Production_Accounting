"""
celery_app.py

Defines the Celery application instance, loads DB,
registers triggers, and then reads tasks from tasks.py
"""
import logging
import sys
from celery import Celery
from database.db_util import initialize_database
from utilities.config import Config
from server_celery.logging_setup import setup_logging
setup_logging()
logger = logging.getLogger('admin_logger')
celery_app = Celery('celery_app', broker='redis://localhost:6379/5', backend='redis://localhost:6379/5')
celery_app.conf.update(worker_prefetch_multiplier=1, task_acks_late=True, worker_concurrency=10, broker_transport_options={'visibility_timeout': 3600})
logger.debug(f'Celery config: worker_prefetch_multiplier={celery_app.conf.worker_prefetch_multiplier}, task_acks_late={celery_app.conf.task_acks_late}')
celery_app.autodiscover_tasks(['celery_tasks'], force=True)

@celery_app.on_after_finalize.connect
def announce_tasks(sender, **kwargs):
    logger.info('Celery tasks have been finalized. Ready to go!')
from celery.signals import worker_init, worker_ready, worker_shutdown

@worker_init.connect
def init_db(**kwargs):
    config = Config()
    db_settings = config.get_database_settings(config.USE_LOCAL)
    try:
        initialize_database(db_settings['url'])
        logger.info('DB initialization is done.')
    except Exception as e:
        logger.error(f'DB initialization failed! Error={e}', exc_info=True)
    logger.info('Initializing DB inside Celery worker...')

@worker_init.connect
def signal_worker_init(sender=None, **kwargs):
    logger.info('👷\u200d♀️ Celery Worker is starting up... Warm up the engines!')

@worker_ready.connect
def signal_worker_ready(sender=None, **kwargs):
    logger.info('🚀 Celery Worker is READY and waiting for tasks! Buckle up, folks.')

@worker_shutdown.connect
def signal_worker_shutdown(sender=None, **kwargs):
    logger.warning('🛑 Celery Worker is shutting down. Everyone, please exit in an orderly fashion.')