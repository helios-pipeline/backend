import subprocess
import sys


def start_db():
    result = subprocess.run(["sh", "scripts/development/clickhouse-setup.sh"], check=True)
    sys.exit(result.returncode)


def start_db_mac():
    result = subprocess.run(
        ["sh", "scripts/development/clickhouse-setup-mac.sh"], check=True
    )
    sys.exit(result.returncode)


def build_image():
    result = subprocess.run(["sh", "scripts/development/build-image.sh"], check=True)
    sys.exit(result.returncode)


def build_flask_image():
    print('test')
    result = subprocess.run(["sh", "scripts/build-flask-image.sh"], check=True)
    sys.exit(result.returncode)


def build_image_mac():
    result = subprocess.run(["sh", "scripts/development/build-image-mac.sh"], check=True)
    sys.exit(result.returncode)


def run_dev():
    result = subprocess.run(["python3", "app/main.py"], check=True)
    sys.exit(result.returncode)


def generate_data():
    result = subprocess.run(["python3", "generate_data2.py"], check=True)
    sys.exit(result.returncode)

