import argparse
import logging

from jpost.etl.scheduler import JPostTaskScheduler
from manhole_card.etl.scheduler import ManholeCardTaskScheduler

SCHEDULERS = {
    "jpost": JPostTaskScheduler,
    "manhole_card": ManholeCardTaskScheduler,
    # Add more schedulers here in the future, e.g.:
    # "other": OtherTaskScheduler,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="TaskScheduler runner entry point."
    )
    parser.add_argument(
        "-s",
        "--scheduler",
        choices=SCHEDULERS.keys(),
        default="jpost",
        help="Scheduler to run (default: jpost).",
    )
    parser.add_argument(
        "-t",
        "--threads",
        type=int,
        default=5,
        help="Number of worker threads (default: 5).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    scheduler_cls = SCHEDULERS[args.scheduler]
    logging.info(
        "Starting scheduler %s with %d threads",
        scheduler_cls.__name__,
        args.threads,
    )
    scheduler_cls.start(args.threads)


if __name__ == "__main__":
    main()

