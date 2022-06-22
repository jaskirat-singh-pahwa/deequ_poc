from typing import Dict

from awsglue.utils import getResolvedOptions


def parse_args(input_args) -> Dict[str, str]:
    args = getResolvedOptions(input_args, [
        "JOB_NAME",
        "CONSTRAINTS_FILE_BUCKET",
        "CONSTRAINTS_FILE_KEY"
    ])

    return args
