#!/usr/bin/env python3
"""A minimal ce-fn handler.

The ce-fn serve runtime invokes a handler by:
  - spawning this process with the function's declared `command`,
  - writing the invocation payload to stdin,
  - injecting declared env vars and resolved secrets into the environment,
  - capturing stdout as the response body and the exit code as the status.

This handler reads the payload, prepends the GREETING env var, and (if present)
appends a marker proving the API_TOKEN secret was injected — without printing
the secret value itself.
"""
import os
import sys


def main() -> int:
    payload = sys.stdin.buffer.read()
    greeting = os.environ.get("GREETING", "hi")
    name = payload.decode("utf-8", "replace").strip() or "world"

    out = f"{greeting}, {name}!"
    if os.environ.get("API_TOKEN"):
        out += " [authenticated]"

    sys.stdout.write(out)
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
