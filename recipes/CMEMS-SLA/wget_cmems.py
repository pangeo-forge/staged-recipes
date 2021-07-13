import os
import sys
import time
import subprocess

from rich import print

env = os.environ.copy()


def main():
    subprocess.run(["sh", "wget_cmems.sh"], env=env)


if __name__ == "__main__":
    start = time.time()
    try:
        main()
    except KeyboardInterrupt:
        elapsed = round(time.time() - start, 1)

        AVG_BYTES = 58086740
        NSOURCES = 8901
        SOURCE_GBS = round((AVG_BYTES*NSOURCES)/1e9, 2)
        TARGET = env["CMEMS_DIRECTORY"]

        years = sorted(os.listdir(TARGET))
        months = sorted(os.listdir(TARGET + f"/{years[-1]}"))
        files = os.listdir(TARGET + f"/{years[-1]}/{months[-1]}")
        files = sorted([f[30:32] for f in files])
        final_day = f"{years[-1]}-{months[-1]}-{files[-1]}"

        nfiles = sum([len(files) for _, _, files in os.walk(TARGET)])
        nbytes = round((AVG_BYTES*nfiles)/1e9, 2)
        remaining = round(((elapsed/60)/nfiles) * (NSOURCES-nfiles), ndigits=2)

        r, m, c = "[red]", "[bold magenta]", "[/]"
        print(
            "\n"
            f"\n {r}Download interrupted after {c}{m}{round(elapsed/60, 2)} minutes{c}"
            f"{r} ({round(elapsed, 1)} seconds).{c}"
            f"\n {r}Of target range (1993-01-01 to 2017-05-15),{c} {m}{final_day}{c}"
            f"{r} was last day reached.{c}"
            f"\n {r}Downloaded {c}{m}{nfiles} {c}{r}of {c}{m}{NSOURCES}{c}{r} files{c}"
            f" {r}(~{nbytes} GBs of ~{SOURCE_GBS} GBs)."
            f"\n {r}Full download would take an additional{c}"
            f"{m} ~{round(remaining/60, 2)} hours{c}{r} (~{remaining} minutes).{c}"
            "\n"
        )
        sys.exit(0)
