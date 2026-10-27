import json, base64
# Read the service account key
key_path = r'C:\Users\muham\Downloads\kumparan_elt\config\kumparan-elt-project-96478b3ced4a.json'
with open(key_path, 'rb') as f:
    content = f.read()
b64 = base64.b64encode(content).decode('utf-8')
# Create the extra field with keyfile_dict
extra = json.dumps({"keyfile_dict": b64})
# Build the docker command
cmd = (
    "docker exec kumparan_airflow_webserver airflow connections "
    "add google_cloud_default "
    "--conn-type google_cloud_platform "
    "--conn-extra " + json.dumps(extra)
)
print(cmd)
# Execute the command
import subprocess
result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
print("STDOUT:", result.stdout)
print("STDERR:", result.stderr)
print("Return code:", result.returncode)
PYEOF