"""APScheduler wrapper for automated house hunting runs."""

import asyncio
import logging
import os
from datetime import datetime

from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from .graph import HouseHunterGraph

logger = logging.getLogger(__name__)


class HouseHunterScheduler:

    def __init__(self):
        jobstores = {
            'default': SQLAlchemyJobStore(url='sqlite:///house_hunter_jobs.db')
        }
        executors = {
            'default': ThreadPoolExecutor(1)
        }
        job_defaults = {
            'coalesce': True,
            'max_instances': 1
        }

        self.scheduler = BackgroundScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults,
            timezone=os.getenv("HOUSE_HUNTER_TIMEZONE", "US/Eastern")
        )

        self._add_jobs()
        logger.info("HouseHunterScheduler initialized")

    def _add_jobs(self):
        schedule_times = [
            (9, '9 AM'),
            (11, '11 AM'),
            (13, '1 PM'),
            (15, '3 PM'),
            (17, '5 PM'),
            (19, '7 PM'),
            (21, '9 PM'),
            (23, '11 PM')
        ]

        for hour, label in schedule_times:
            self.scheduler.add_job(
                func=self.run_house_hunter,
                trigger=CronTrigger(hour=hour, minute=0),
                id=f'run_{hour}',
                name=f'House Hunt - {label}',
                replace_existing=True
            )

        logger.info("Scheduled jobs added: 9 AM, 11 AM, 1 PM, 3 PM, 5 PM, 7 PM, 9 PM, 11 PM daily")

    def run_house_hunter(self):
        try:
            logger.info(f"Starting scheduled run at {datetime.now()}")

            graph = HouseHunterGraph()
            result = asyncio.run(graph.run(test_mode=False))

            logger.info("Scheduled run completed:")
            logger.info(f"  - Found: {len(result.get('properties', []))}")
            logger.info(f"  - Passed: {len(result.get('passed_properties', []))}")
            logger.info(f"  - Notified: {len(result.get('notified_properties', []))}")

        except Exception as e:
            logger.error(f"Scheduled run failed: {e}")

    def start(self):
        self.scheduler.start()
        logger.info("Scheduler started")

    def stop(self):
        self.scheduler.shutdown()
        logger.info("Scheduler stopped")

    def get_jobs(self):
        return self.scheduler.get_jobs()

    def run_now(self):
        logger.info("Triggering immediate run")
        self.run_house_hunter()
