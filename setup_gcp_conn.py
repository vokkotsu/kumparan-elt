import json
import subprocess

# Container path to the service account key (./config is mounted to /opt/airflow/config)
key_path = "/opt/airflow/config/kumparan-elt-project-96478b3ced4a.json"

extra = {
    "project": "kumparan-elt-project",
    "key_path": key_path,
    "scope": "https://www.googleapis.com/auth/cloud-platform",
}

cmd = [
    "docker", "exec", "kumparan_airflow_webserver",
    "airflow", "connections", "add", "google_cloud_default",
    "--conn-type", "google_cloud_platform",
    "--conn-extra", json.dumps(extra),
]

result = subprocess.run(cmd, capture_output=True, text=True)
print("STDOUT:", result.stdout)
print("STDERR:", result.stderr)
print("Return code:", result.returncode)
