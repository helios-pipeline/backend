import subprocess
import sys


def start_db():
    result = subprocess.run(["sh", "database/scripts/clickhouse-setup.sh"], check=True)
    sys.exit(result.returncode)


def start_db_mac():
    result = subprocess.run(
        ["sh", "database/scripts/clickhouse-setup-mac.sh"], check=True
    )
    sys.exit(result.returncode)


def build_image():
    result = subprocess.run(["sh", "database/scripts/build-image.sh"], check=True)
    sys.exit(result.returncode)


def build_image_mac():
    result = subprocess.run(["sh", "database/scripts/build-image-mac.sh"], check=True)
    sys.exit(result.returncode)


def run_dev():
    result = subprocess.run(["python3", "app/main.py"], check=True)
    sys.exit(result.returncode)


def generate_data():
    result = subprocess.run(["python3", "generate_data.py"], check=True)
    sys.exit(result.returncode)

