from airflow.models import Variable
import subprocess
import sys

def install_package(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

packages = None
try:
    packages = Variable.get("python_packages")
except:
    pass

if packages:
    for l in packages.splitlines():
        try:
            install_package(l)
        except:
            pass
