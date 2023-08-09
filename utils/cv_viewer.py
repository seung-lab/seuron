import sys
from airflow.models import Variable
from cloudvolume.server import view

local_path = sys.argv[1]

if not local_path.endswith("/"):
    local_path += "/"

ng_subs = {
            "old": local_path,
            "new": "http://localhost:8080/"
          }

Variable.set("ng_subs", ng_subs, serialize_json=True)
view(sys.argv[1], hostname="0.0.0.0")
