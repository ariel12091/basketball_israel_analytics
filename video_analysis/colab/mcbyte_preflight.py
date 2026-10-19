"""Diagnose the McByte++ Colab environment before starting tracking."""

from pathlib import Path
import os
import subprocess


WORK = Path("/content/ibpl_mcbyte")
REPO = WORK / "McBytePlusPlus"
PYTHON = WORK / "venv" / "bin" / "python"


def headless_environment() -> dict[str, str]:
    """Use a backend available inside the isolated non-notebook process."""
    environment = os.environ.copy()
    environment["MPLBACKEND"] = "Agg"
    return environment


def run_preflight() -> None:
    if not PYTHON.exists():
        raise FileNotFoundError(
            f"Python environment not found at {PYTHON}. "
            "Run the notebook installation cells first."
        )
    if not REPO.exists():
        raise FileNotFoundError(
            f"McByte++ repository not found at {REPO}. "
            "Run the notebook clone cell first."
        )

    cuda_check = (
        "import torch,numpy,cv2; "
        "print('torch', torch.__version__, "
        "'cuda', torch.version.cuda, "
        "'gpu', torch.cuda.get_device_name(0), "
        "'vram_GB', "
        "round(torch.cuda.get_device_properties(0).total_memory / 2**30, 2), "
        "'numpy', numpy.__version__)"
    )
    subprocess.run(
        [str(PYTHON), "-c", cuda_check],
        cwd=REPO,
        check=True,
        env=headless_environment(),
    )

    probe = subprocess.run(
        [str(PYTHON), "tools/demo_track__with_reid.py", "--help"],
        cwd=REPO,
        text=True,
        capture_output=True,
        env=headless_environment(),
    )

    if probe.stdout:
        print(probe.stdout)

    if probe.returncode != 0:
        print("--- McByte++ import error (stderr) ---")
        print(probe.stderr)
        raise SystemExit(probe.returncode)

    print("McByte++ preflight passed.")


if __name__ == "__main__":
    run_preflight()
