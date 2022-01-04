from airflow.models import Variable
import subprocess
import sys

def install_package(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

Variable.setdefault("python_packages","")
packages = Variable.get("python_packages")

if packages:
    for l in packages.splitlines():
        try:
            install_package(l)
        except:
            pass
