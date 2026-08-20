#!/usr/bin/env python3
"""POC demo for the OpenKruise Agents Substrate backend.

It walks the full E2B flow against a sandbox-manager started with
``--substrate-addr``:

1. Build an ActorTemplate through the three-phase E2B template build API.
2. Poll the build until it is ready.
3. Create a sandbox (Substrate actor) from the template, optionally pinned to a
   specific SandboxSet's worker pool.
4. Pause, resume and kill the sandbox.

The build API is a plain REST surface, so this demo uses ``requests`` directly
rather than the E2B SDK: the SDK's high-level ``Template.build`` assumes a build
pipeline (RUN/COPY steps), while the Substrate backend only accepts a prebuilt,
digest-pinned ``fromImage``.

Usage:
    export E2B_DOMAIN=sandbox-manager.example.com:8080
    export E2B_API_KEY=<team-api-key>
    python demo.py \
        --template counter \
        --from-image registry.example.com/counter@sha256:<digest> \
        --start-cmd "/ko-app/counter" \
        --ready-cmd "http://localhost:8080/readyz" \
        --sandboxset counter
"""

import argparse
import os
import sys
import time

import requests
from e2b import Sandbox

# Metadata key that pins a sandbox to a specific SandboxSet's worker pool. It is
# an E2B extension key parsed by sandbox-manager (the e2b. prefix is otherwise
# reserved), so it is passed through Sandbox.create's metadata argument.
SANDBOXSET_METADATA_KEY = "e2b.agents.kruise.io/sandboxset"


def api_base(domain: str) -> str:
    scheme = "http" if ("localhost" in domain or ":8080" in domain) else "https"
    return f"{scheme}://{domain}"


def build_template(session: requests.Session, base: str, args) -> None:
    """Run the three-phase E2B template build against the substrate backend."""
    # Phase 1: reserve the template identity and a build slot.
    resp = session.post(
        f"{base}/v3/templates",
        json={"name": args.template, "cpuCount": args.cpu, "memoryMB": args.memory},
        timeout=30,
    )
    resp.raise_for_status()
    reserved = resp.json()
    template_id, build_id = reserved["templateID"], reserved["buildID"]
    print(f"[phase1] reserved template={template_id} build={build_id}")

    # Phase 2: submit the build definition. Substrate turns this into an
    # immutable ActorTemplate; steps are rejected, so only fromImage is sent.
    start = {"fromImage": args.from_image}
    if args.start_cmd:
        start["startCmd"] = args.start_cmd
    if args.ready_cmd:
        start["readyCmd"] = args.ready_cmd
    resp = session.post(
        f"{base}/v2/templates/{template_id}/builds/{build_id}",
        json=start,
        timeout=30,
    )
    resp.raise_for_status()
    print(f"[phase2] build started with image {args.from_image}")

    # Phase 3: poll until the ActorTemplate golden snapshot is ready.
    deadline = time.time() + args.build_timeout
    while time.time() < deadline:
        resp = session.get(
            f"{base}/templates/{template_id}/builds/{build_id}/status",
            timeout=30,
        )
        resp.raise_for_status()
        info = resp.json()
        status = info.get("status")
        print(f"[phase3] build status: {status}")
        if status == "ready":
            print(f"[phase3] template {template_id} is ready")
            return
        if status == "error":
            logs = "\n".join(info.get("logs") or [])
            sys.exit(f"build failed: {logs}")
        time.sleep(3)
    sys.exit("timed out waiting for the template build to become ready")


def run_sandbox_lifecycle(args) -> None:
    """Create, pause, resume and kill a sandbox from the built template."""
    metadata = {}
    if args.sandboxset:
        # Pin placement to a specific SandboxSet's worker pool. Omit to let the
        # actor land on any eligible pool in the namespace.
        metadata[SANDBOXSET_METADATA_KEY] = args.sandboxset

    print(f"[create] creating sandbox from template {args.template} metadata={metadata}")
    sbx = Sandbox.create(
        template=args.template,
        metadata=metadata,
        domain=args.domain,
        api_key=args.api_key,
    )
    print(f"[create] sandbox running: {sbx.sandbox_id}")

    print("[pause] pausing sandbox (suspend frees the worker)")
    sbx.pause()

    print("[resume] resuming sandbox from snapshot")
    sbx.connect(sbx.sandbox_id, domain=args.domain, api_key=args.api_key)

    print("[kill] deleting sandbox")
    sbx.kill()
    print("[done] lifecycle complete")


def main() -> None:
    parser = argparse.ArgumentParser(description="Substrate backend POC demo")
    parser.add_argument("--domain", default=os.environ.get("E2B_DOMAIN"),
                        help="sandbox-manager host:port (env E2B_DOMAIN)")
    parser.add_argument("--api-key", default=os.environ.get("E2B_API_KEY"),
                        help="team API key (env E2B_API_KEY)")
    parser.add_argument("--template", required=True, help="template name to build and run")
    parser.add_argument("--from-image", required=True,
                        help="digest-pinned base image (must contain @sha256:...)")
    parser.add_argument("--start-cmd", default="", help="command to start in the sandbox")
    parser.add_argument("--ready-cmd", default="",
                        help="HTTP readiness check, e.g. http://localhost:8080/readyz")
    parser.add_argument("--sandboxset", default="",
                        help="optional SandboxSet name to pin worker-pool placement")
    parser.add_argument("--cpu", type=int, default=2, help="template cpuCount (echoed only)")
    parser.add_argument("--memory", type=int, default=1024, help="template memoryMB (echoed only)")
    parser.add_argument("--build-timeout", type=int, default=300,
                        help="seconds to wait for the build to become ready")
    parser.add_argument("--skip-build", action="store_true",
                        help="skip the build phase and only run the sandbox lifecycle")
    args = parser.parse_args()

    if not args.domain or not args.api_key:
        sys.exit("E2B_DOMAIN and E2B_API_KEY (or --domain/--api-key) are required")
    if "@" not in args.from_image:
        sys.exit("--from-image must be pinned by digest (contain '@sha256:...')")

    session = requests.Session()
    session.headers.update({"X-API-Key": args.api_key})

    if not args.skip_build:
        build_template(session, api_base(args.domain), args)
    run_sandbox_lifecycle(args)


if __name__ == "__main__":
    main()
