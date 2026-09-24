from __future__ import annotations

import argparse

from loadtest.settings import LoadTestSettings


def cmd_create_schema(args, settings: LoadTestSettings) -> int: ...


def cmd_generate(args, settings: LoadTestSettings) -> int: ...


def cmd_load(args, settings: LoadTestSettings) -> int: ...


def cmd_sync(args, settings: LoadTestSettings) -> int: ...


def cmd_validate(args, settings: LoadTestSettings) -> int: ...


def cmd_all(args, settings: LoadTestSettings) -> int: ...


def build_parser() -> argparse.ArgumentParser: ...


def main(argv: list[str] | None = None) -> int: ...
