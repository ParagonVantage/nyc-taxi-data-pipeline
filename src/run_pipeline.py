import subprocess
import sys
from pathlib import Path

def run(cmd):
    print("\n>>>", " ".join(cmd))
    subprocess.check_call(cmd)

def main():
    ROOT = Path(__file__).resolve().parents[1]

    run([sys.executable, str(ROOT / "src" / "download_data.py")])
    run([sys.executable, str(ROOT / "src" / "export_parquet.py")])
    run([sys.executable, str(ROOT / "src" / "spark_mart.py")])
    run([sys.executable, str(ROOT / "src" / "publish_marts.py")])

    print("\nPipeline complete. Published tables are in data/published/")

if __name__ == "__main__":
    main()
