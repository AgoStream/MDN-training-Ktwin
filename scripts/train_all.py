import subprocess
import sys
from pathlib import Path
import os

services = ["frontend", "auth", "bill", "ms1", "ms2"]
root_dir = Path(__file__).resolve().parent
project_root = root_dir.parent  # cartella che contiene "src/"

if len(sys.argv) < 2:
    print("Usage: python train_all.py <data_file>")
    sys.exit(1)

data_file = sys.argv[1]

for service in services:
    print(f"\n=== Training servizio: {service} ===")

    result = subprocess.run(
        [sys.executable, "src/train/train.py", service, data_file],
        cwd=project_root,  # cambia working dir alla root del progetto
        env={**os.environ, "PYTHONPATH": str(project_root)},  # imposta PYTHONPATH
    )

    if result.returncode != 0:
        print(f"Error during training of: {service}")
        break
