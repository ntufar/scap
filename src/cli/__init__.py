import argparse
from src.lib.logging import get_logger


def main() -> None:
    logger = get_logger("cli")
    parser = argparse.ArgumentParser(prog="scap")
    parser.add_argument("command", choices=["load_sample_data", "run_pipeline"], help="Command to run")
    parser.add_argument("--stage", choices=["bronze", "silver", "gold"], required=False)
    parser.add_argument("--domain", choices=["supplier", "logistics", "inventory"], required=False)
    args = parser.parse_args()

    logger.info(f"Command received: {args.command} stage={args.stage} domain={args.domain}")


if __name__ == "__main__":
    main()


