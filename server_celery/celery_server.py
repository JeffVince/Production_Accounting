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
from dotenv import load_dotenv
load_dotenv("../.env")

celery_app = Celery(
    'celery_app',
    broker='redis://localhost:6379/5',
    backend='redis://localhost:6379/5'
)

celery_app.conf.update(
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    worker_concurrency=3,
    broker_transport_options={'visibility_timeout': 3600}
)

logger.debug(
    f'Celery config: '
    f'worker_prefetch_multiplier={celery_app.conf.worker_prefetch_multiplier}, '
    f'task_acks_late={celery_app.conf.task_acks_late}'
)

celery_app.autodiscover_tasks(['server_celery.celery_tasks'], force=True)

@celery_app.on_after_finalize.connect
def announce_tasks(sender, **kwargs):
    logger.info('Celery tasks have been finalized. Ready to go!')

from celery.signals import worker_init, worker_ready, worker_shutdown

@worker_init.connect
def init_db(**kwargs):
    logger.info('Initializing Local DB session inside Celery worker...')

    config = Config()
    db_settings = config.get_database_settings(config.USE_LOCAL)
    try:
        initialize_database(db_settings['url'])
        logger.info('DB in itialization is done.')
    except Exception as e:
        logger.error(f'DB initialization failed! Error={e}', exc_info=True)

@worker_init.connect
def signal_worker_init(sender=None, **kwargs):
    """
    Runs at worker initialization. We also purge any leftover tasks
    so the queue is empty when we start accepting new tasks.
    """
    logger.info('👷\u200d♀️ Celery Worker is starting up... Warm up the engines!')
    try:
        pass
        #purged_count = celery_app.control.purge()
        #logger.warning(f'Purged {purged_count} tasks from the queue at startup.')
    except Exception as e:
        logger.error(f'Error while purging tasks at startup: {e}', exc_info=True)

@worker_ready.connect
def signal_worker_ready(sender=None, **kwargs):
    logger.info('🚀 Celery Worker is READY and waiting for tasks! Buckle up, folks.')

@worker_shutdown.connect
def signal_worker_shutdown(sender=None, **kwargs):
    """
    Runs just before the Celery worker fully shuts down.
    We revoke any active tasks (forcing them to stop) and then purge
    any remaining messages in the queue.
    """
    logger.warning('🛑 Celery Worker is shutting down. Everyone, please exit in an orderly fashion.')

    try:
        # Revoke active tasks
        i = celery_app.control.inspect()
        active_tasks = i.active()
        if active_tasks:
            logger.warning('Revoking all active tasks...')
            for worker_name, tasks in active_tasks.items():
                for t in tasks:
                    task_id = t['id']
                    # terminate=True signals the worker to forcefully kill the task
                    celery_app.control.revoke(task_id, terminate=True, signal='SIGKILL')
            logger.warning('All active tasks have been revoked.')
        else:
            logger.info('No active tasks to revoke.')

        # Purge tasks from the queue
        purged_count = celery_app.control.purge()
        logger.warning(f'Purged {purged_count} tasks from the queue on shutdown.')
    except Exception as e:
        logger.error(f'Error while purging tasks during shutdown: {e}', exc_info=True)