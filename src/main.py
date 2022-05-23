import sys

from src.args import parse_args


def main(argv) -> None:
    args = parse_args(argv)
    print(args["claims_data_path"])


if __name__ == "__main__":
    main(sys.argv[1:])
