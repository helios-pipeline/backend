import subprocess
import sys


def start_db():
    result = subprocess.run(["sh", "database/scripts/clickhouse-setup.sh"], check=True)
    sys.exit(result.returncode)


def build_image():
    result = subprocess.run(["sh", "database/scripts/build-image.sh"], check=True)
    sys.exit(result.returncode)
