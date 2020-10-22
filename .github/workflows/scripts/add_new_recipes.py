"""
Add new recipes to pangeo-forge's GitHub organization.
"""
import argparse
import os
import pathlib
import shutil
import subprocess
import sys
from typing import List

import requests

BASE = "https://api.github.com"
HEADERS = {
    "Accept": "application/vnd.github.v3+json",
}


def list_remote_pipelines() -> List[str]:
    """List all the repositories in the pangeo-forge organization."""
    r = requests.get(f"{BASE}/orgs/pangeo-forge/repos", headers=HEADERS)
    r.raise_for_status()
    repos = r.json()
    repos = [repo["name"] for repo in repos if "-pipeline" in repo["name"]]
    return repos


def list_local_pipelines() -> List[pathlib.Path]:
    """List all the pipelines in staged-recipes."""
    recipes = [x for x in pathlib.Path("recipes/").iterdir() if x.is_dir()]
    return recipes


def create_repository(repository: pathlib.Path):
    root = pathlib.Path(".").joinpath("..", f"{repository.name}-pipeline")
    root.mkdir()
    shutil.copytree(repository, root / "recipe")
    workflows = root / ".github/workflows/"
    workflows.mkdir(parents=True, exist_ok=True)
    shutil.copy(pathlib.Path(".") / ".github/workflows/scripts/register_pipeline.yaml", workflows)
    root.joinpath("README.md").touch()  # TODO: template.

    subprocess.check_output(["git", "-C", str(root), "init"])
    subprocess.check_output(["git", "-C", str(root), "add", "-A"])
    subprocess.check_output(["git", "-C", str(root), "commit", "-m", "initial commit"])

    assert "GH_API_TOKEN" in os.environ
    GH_TOKEN = os.environ["GH_TOKEN"]
    auth = ("pangeo-bot", GH_TOKEN)
    data = dict(
        name=f"{repository.name}-pipeline",
        description=f"pangeo-forge pipeline for {repository.name}",
        homepage="https://pangeo.io",
    )
    r = requests.post(f"{BASE}/orgs/pangeo-forge/repos", json=data, headers=HEADERS, auth=auth)
    r.raise_for_status()
    url = f"https://x-access-token:{GH_TOKEN}@github.com/pangeo-forge/{repository.name}-pipeline"

    print("Created repository", url)
    subprocess.check_output(["git", "remote", "set-url", "origin", url])
    subprocess.check_output(["git", "-C", str(root), "push", "--set-upstream", "origin", "master"])
    print("Pushed repository")


def parse_args(args=None):
    parser = argparse.ArgumentParser(__name__, usage=__doc__)
    return parser.parse_args(args)


def main(args=None):
    args = parse_args(args)
    local_pipelines = list_local_pipelines()
    remote_pipelines = list_remote_pipelines()
    local_pipelines = {f"{p.name}-pipeline": p for p in local_pipelines}

    new_pipelines = set(local_pipelines) - set(remote_pipelines)
    for new_pipeline in new_pipelines:
        print("Creating new pipeline", new_pipeline)
        create_repository(local_pipelines[new_pipeline])

    # TODO: remove from staged-recipes


if __name__ == "__main__":
    sys.exit(main())
