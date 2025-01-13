# -*- coding: utf-8 -*-
"""
celery_app.py

Defines the Celery application instance, loads DB,
registers triggers, and then reads tasks from tasks.py
"""

import logging
from celery import Celery
from kombu import Queue, Exchange

from db_util import initialize_database
from utilities.config import Config

logger = logging.getLogger("app_logger")
logger.setLevel(logging.DEBUG)

################################################################################
#  RECOMMENDED CELERY CONFIGURATIONS
################################################################################

################################################################################

celery_app = Celery(
    'celery_app',
    broker='redis://localhost:6379/0',
    backend='redis://localhost:6379/0'
)

# Example Celery config settings:
celery_app.conf.update(
    worker_prefetch_multiplier=1,  # Pull tasks 1 at a time per worker
    task_acks_late=True,           # So tasks are not lost if a worker dies mid-task
    # You can also set concurrency if you want a specific number of worker processes:
    worker_concurrency=10,
    # broker_transport_options={'visibility_timeout': 3600},  # Helpful for Redis in some cases
)




logger.debug(f"Celery config: worker_prefetch_multiplier={celery_app.conf.worker_prefetch_multiplier}, "
             f"task_acks_late={celery_app.conf.task_acks_late}")




# Load tasks from tasks.py
celery_app.autodiscover_tasks(['celery_tasks'], force=True)
# or you can do: from tasks import process_invoice_trigger, etc.

@celery_app.on_after_finalize.connect
def announce_tasks(sender, **kwargs):
    logger.info("Celery tasks have been finalized. Ready to go!")

# Optional signals:
from celery.signals import worker_init, worker_ready, worker_shutdown

@worker_init.connect
def init_db(**kwargs):
    logger.info("Initializing DB inside Celery worker...")

@worker_init.connect
def signal_worker_init(sender=None, **kwargs):
    logger.info("👷‍♀️ Celery Worker is starting up... Warm up the engines!")

@worker_ready.connect
def signal_worker_ready(sender=None, **kwargs):
    logger.info("🚀 Celery Worker is READY and waiting for tasks! Buckle up, folks.")

@worker_shutdown.connect
def signal_worker_shutdown(sender=None, **kwargs):
    logger.warning("🛑 Celery Worker is shutting down. Everyone, please exit in an orderly fashion.")