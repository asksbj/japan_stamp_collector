import argparse
import logging
import os
import sys

import uvicorn

from main import app
from task_scheduler import main as run_scheduler


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Container entrypoint for japan_stamp_collector."
    )
    parser.add_argument(
        "--mode",
        choices=["web", "scheduler"],
        default=os.getenv("APP_MODE", "web"),
        help="Run mode: 'web' (FastAPI) or 'scheduler' (TaskScheduler).",
    )
    parser.add_argument(
        "--host",
        default=os.getenv("APP_HOST", "0.0.0.0"),
        help="Host for FastAPI server (default: 0.0.0.0).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("APP_PORT", "8000")),
        help="Port for FastAPI server (default: 8000).",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=int(os.getenv("SCHEDULER_THREADS", "5")),
        help="Number of worker threads when running scheduler (default: 5).",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    args = parse_args()

    if args.mode == "web":
        uvicorn.run(app, host=args.host, port=args.port)
    elif args.mode == "scheduler":
        # delegate to existing task_scheduler entrypoint
        sys.argv = ["task_scheduler.py", "--threads", str(args.threads)]
        run_scheduler()
    else:
        raise ValueError(f"Unsupported mode: {args.mode}")


if __name__ == "__main__":
    main()

