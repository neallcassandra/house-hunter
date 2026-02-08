"""Main entry point for the House Hunter system."""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

from .graph import HouseHunterGraph
from .scheduler import HouseHunterScheduler

# Ensure logs directory exists
project_root = Path(__file__).parent.parent
logs_dir = project_root / "logs"
logs_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(logs_dir / 'house_hunter.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

load_dotenv()


async def run_once(test_mode: bool = False):
    """Run the house hunter workflow once."""
    try:
        logger.info("=" * 60)
        logger.info("🏠 HOUSE HUNTER - SINGLE RUN")
        logger.info("=" * 60)

        graph = HouseHunterGraph()
        result = await graph.run(test_mode=test_mode)

        print("\n" + "=" * 60)
        print("HOUSE HUNTER RESULTS")
        print("=" * 60)
        print(f"Run ID: {result.get('run_id')}")
        print(f"Properties Found: {len(result.get('properties', []))}")
        print(f"Properties Passed Review: {len(result.get('passed_properties', []))}")
        print(f"Notifications Sent: {len(result.get('notified_properties', []))}")
        print(f"API Calls Used: {result.get('api_calls_used')}/{result.get('api_calls_limit')}")

        if result.get('errors'):
            print(f"\nErrors: {len(result['errors'])}")
            for error in result['errors']:
                print(f"  - {error['node']}: {error['error']}")

        print("=" * 60)

        return result

    except Exception as e:
        logger.error(f"Error running house hunter: {e}")
        raise


def run_scheduler():
    try:
        logger.info("=" * 60)
        logger.info("🏠 HOUSE HUNTER - SCHEDULER MODE")
        logger.info("=" * 60)

        scheduler = HouseHunterScheduler()

        print("\n" + "=" * 60)
        print("HOUSE HUNTER SCHEDULER STARTED")
        print("=" * 60)
        print("Schedule: 8:00 AM and 6:00 PM daily")
        print("Press Ctrl+C to stop")
        print("=" * 60 + "\n")

        scheduler.start()

        try:
            while True:
                asyncio.run(asyncio.sleep(1))
        except KeyboardInterrupt:
            logger.info("Shutting down scheduler...")
            scheduler.stop()

    except Exception as e:
        logger.error(f"Scheduler error: {e}")
        raise


def main():
    parser = argparse.ArgumentParser(description="House Hunter Agent System")
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    parser.add_argument("--test", action="store_true", help="Run in test mode (no notifications)")
    parser.add_argument("--scheduler", action="store_true", help="Start the scheduler for automated runs")
    parser.add_argument("--stats", action="store_true", help="Show database statistics")

    args = parser.parse_args()

    try:
        if args.stats:
            from .database import PropertyDatabase
            db = PropertyDatabase()
            stats = db.get_statistics()

            print("\n" + "=" * 60)
            print("HOUSE HUNTER STATISTICS")
            print("=" * 60)
            print(f"Total Properties Seen: {stats.get('total_properties', 0)}")
            print(f"Properties Notified: {stats.get('properties_notified', 0)}")
            print(f"Properties Passed Review: {stats.get('properties_passed', 0)}")
            print(f"Last 7 Days: {stats.get('last_7_days', 0)}")

            if stats.get('by_city'):
                print("\nBy City:")
                for city, count in stats['by_city'].items():
                    print(f"  - {city}: {count}")

            print("=" * 60)

        elif args.scheduler:
            run_scheduler()

        elif args.once or args.test:
            asyncio.run(run_once(test_mode=args.test))

        else:
            parser.print_help()

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
