import json, base64
b64 = base64.b64encode(open('C:\\Users\\muham\\Downloads\\kumparan_elt\\config\\kumparan-elt-project-96478b3ced4a.json', 'rb').read()).decode('utf-8')
extra = json.dumps({"keyfile_dict": b64})
cmd = 'docker exec kumparan_airflow_webserver airflow connections add google_cloud_default --conn-type google_cloud_platform --conn-extra ' + json.dumps(extra)
print(cmd)
PYEOF