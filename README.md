# AirflowTask

## How to run:
- make sure Docker is installed and Docker daemon is running
- cd into this directory
- Execute:
```run docker compose up```

### Next, we need to insert slack token into vault:
- find container id of vault (use docker ps)
- Execute:
```
docker exec -it VAULT_DOCKER_ID sh
vault login ZyrP7NtNw0hbLUqu7N3IlTdO
vault secrets enable -path=airflow -version=2 kv
vault kv put airflow/variables/slack_token value=xoxb-7469760031683-7492974501376-3giEHbwG3x825nhVw7vUROyn
```

After that go to localhost:8080, created dags will be there (username: airflow, password: airflow).

Trigger_dag needs a file test.txt to be created, in order to start. To create this file:
- find id of worker
- Execute:
```bash
docker exec -it WORKER_DOCKER_ID /bin/bash
cd /tmp
touch test.txt
```