## Pirates Streams Demo — README
A short, clear guide that explains what this repo contains and how I moved the ETL from local → Docker → Kubernetes, 
what I changed to make Kafka reachable from both Docker and K8s, and how to run / debug the system.

### Overview
This mini-project demonstrates a small streaming ETL:
1. Producer (local script) → sends pitch events to Kafka
2. Kafka + Zookeeper running in Docker (docker-compose)
3. ETL consumer (Python) that reads from Kafka, stores raw events to MinIO and inserts transformed rows into Postgres
4. Postgres and MinIO running in Docker
5. Optionally: ETL deployed to Kubernetes (Docker Desktop / EKS / GKE pattern)

### Files you will find in the repo:
1. docker-compose.yml — local stack (Zookeeper, Kafka, Postgres, MinIO, Kafka-UI, ETL service)
2. Dockerfile — ETL image
3. etl_consumer.py (or etl.py) — ETL code
4. k8s/etl-deployment.yaml — example Kubernetes manifest for the ETL
5. README.md (this file)

### Goals of this Project
**This project demonstrates:**
1. Running a full streaming data pipeline locally
2. Containerizing the ETL service with Docker
3. Deploying ETL to Kubernetes
4. Understanding Kafka networking and advertised listeners
5. Debugging distributed system connectivity

### Architecture Overview
Producer (local Python script)\
        │
        ▼
Kafka (Docker container)\
        │
        ▼
ETL Consumer (Docker → later deployed to Kubernetes)
        │
        ├──► PostgreSQL (structured analytics data)
        │
        └──► MinIO (raw event storage / S3-style storage)
        
| Component        | Technology |
| ---------------- | ---------- |
| Streaming        | Kafka      |
| ETL              | Python     |
| Database         | PostgreSQL |
| Object Storage   | MinIO      |
| Containerization | Docker     |
| Orchestration    | Kubernetes |
| Debugging UI     | Kafka UI   |

### Data Flow
- The producer script generates simulated pitch events.
- Events are published to the Kafka topic player-events.
- The ETL consumer reads events from Kafka.
- The ETL performs validation and transformation.
- Processed data is stored in:\
  PostgreSQL for structured analytics\
  MinIO for raw event archival.

### Deployment Phases

**This project was implemented in two stages:**

**Phase 1** — Local Docker Deployment
- All services were run locally using Docker Compose.

**Phase 2** — Hybrid Deployment
- Kafka, PostgreSQL, and MinIO remained in Docker.
- The ETL consumer was deployed to Kubernetes, simulating a production-like architecture where application services run in container orchestration platforms.

### 1 — Run everything locally with Docker Compose
This is the fastest way to run the whole system on a single machine.

**1.1 Start the stack**
in repo root (cmd)
docker compose up -d

**This starts:**
- zookeeper (2181 → host port 22181)
- kafka (internal 9092, external port mapped 29092)
- postgres (5432 → host 62000)
- minio (9000, console 9001)
- kafka-ui (8080)
- (optional) etl container if defined in compose

**1.2 Check containers**
(cmd)
docker compose ps
or
docker ps

**1.3 Use Kafka locally**
- Producer script running on your Mac/Linux should use:
bootstrap_servers = "localhost:29092"
This hits the Kafka broker via the host port mapped by Docker Compose.

- From another container (inside compose network) use:
kafka:9092

### 2 — Dockerize the ETL and build the image
If ETL is run from your local machine, Dockerize it and push an image so K8s can pull it.

**2.1 Build locally**
(cmd)\
docker build -t yourdockerhub/pirates-etl:1.0 .\
Make sure Dockerfile is present in the directory where you run the command.

**2.2 Push to a registry**
You can push to Docker Hub, ECR, GCR, etc.\
(cmd)\
docker tag pirates-etl:1.0 yourdockerhub/pirates-etl:1.0\
docker push yourdockerhub/pirates-etl:1.0\
If you use a cloud registry (ECR/GCR), follow the provider’s login/push steps.

### 3 — Deploy ETL to Kubernetes
I used Docker Desktop’s Kubernetes during development, but the same manifests apply to EKS/GKE with small changes.

**3.1 Example etl-deployment.yaml (concept)**
Your Deployment needs:
- the pushed image (full registry path)
- env vars for Kafka / Postgres / MinIO (no plaintext secrets for production — use K8s Secrets)
- liveness/readiness probes and resource limits (recommended)
Example kube command:

kubectl apply -f k8s/etl-deployment.yaml
kubectl get pods -w
kubectl logs -f deployment/etl

Important: set imagePullPolicy: Always on the deployment so K8s pulls the latest image.



### 4 — Kafka LISTENERS & ADVERTISED_LISTENERS — what changed and why
Kafka networking requires two different listener configs when you want the broker reachable from both inside Docker (other containers / K8s pods) and from the host (dev machine) or other external clients:

**4.1 What they are:**
- KAFKA_LISTENERS — where Kafka binds on the machine (addresses/ports Kafka listens on)
- KAFKA_ADVERTISED_LISTENERS — the addresses Kafka tells clients to connect to (the broker advertises these to clients)

You can define multiple listeners, e.g. INTERNAL and EXTERNAL.

**4.2 Example used in compose:**

KAFKA_LISTENERS: INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:29092
KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:9092,EXTERNAL://localhost:29092
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL

- INTERNAL://kafka:9092 — internal Docker network clients (other containers, or K8s when using cluster DNS) should use this
- EXTERNAL://localhost:29092 — producers running on your host (macOS) can use localhost:29092 to reach the broker

**4.3 Why this matters when deploying ETL to Kubernetes**
- When ETL was a container in the same Docker compose network, it could use kafka:9092.
- When ETL runs in K8s, pods have their own network namespace. To connect back to your Dockerized Kafka (on your laptop), 
- you cannot use localhost from inside the pod.
A common solution on Docker Desktop is to use host.docker.internal:29092 
(a special DNS mapping provided by Docker Desktop) in KAFKA_ADVERTISED_LISTENERS so K8s pods can reach Kafka via the host IP:
  EXTERNAL://host.docker.internal:29092
- In some environments host.docker.internal may not resolve (or works only on certain OS). 
In that case use a stable host IP that both Mac, Docker and K8s can see (my.ip.address:29092).

**4.4 Typical changes I made during debugging:**

- Originally: EXTERNAL://localhost:29092 — this worked for host-based producers, but K8s pods could not resolve localhost to host machine.
- Changed to: EXTERNAL://host.docker.internal:29092 — allowed pods to connect (Docker Desktop provides this name).
- On mac, sometimes host.docker.internal didn’t resolve from some contexts — I switched to 
EXTERNAL://<my-machine-ip>:29092 and changed producer bootstrap server accordingly. That is the most robust for mixed environments.

### 5 — ETL environment variables (examples)
When ETL runs inside K8s, pass connection strings as env vars (in Deployment or via Secrets):\
view etl-deployement.yaml in code

### 6 — Verify everything is working
**Docker checks** \
(cmd)\
docker compose ps\
docker logs kafka\
docker exec -it kafka bash   # to run kafka CLI inside broker container

**Kafka topic and consumer group checks (from host, or inside kafka container)**
  
**describe consumer groups**\
(cmd)\
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:29092 --describe --group etl-group

**describe topic config**\
(cmd)\
docker exec -it kafka kafka-configs --bootstrap-server localhost:29092 --entity-type topics --entity-name player-events --describe

**Kubernetes checks**\
(cmd)\
kubectl get pods\
kubectl logs deployment/etl\
kubectl describe pod <pod-name>\
kubectl exec -it <pod-name> -- /bin/sh   # debug inside pod\
*Test connectivity from pod (if busybox/netcat is present)*\
kubectl exec -it <pod-name> -- python -c "import socket; socket.create_connection(('host.docker.internal',29092),5)"

If Kubernetes pod cannot reach the broker, try:
- getent hosts host.docker.internal (inside pod)
- Ping the machine IP from pod (some clusters block ping)
- Replace host.docker.internal with the machine IP in both KAFKA_ADVERTISED_LISTENERS and ETL KAFKA_BROKER

### 7 — Common gotchas & troubleshooting
- host.docker.internal not resolving: use the laptop IP (e.g. 192.168.x.y) instead.
- Producer sends but Kafka has no new messages: check KAFKA_ADVERTISED_LISTENERS — clients may be connecting to the broker, but the broker advertises an address clients cannot reach.
- ETL in k8s connects, but records not in Postgres: check ETL logs, DB host/port env vars, and confirm Postgres is reachable from pod.
- Consumer group shows no active members: confirms consumer not connected or consumer-id disconnected. Use kubectl logs / docker logs to check ETL’s consumer startup messages.
- Docker Compose up blocked when you Ctrl+C: use docker compose up -d to detach and background the stack.
- Producer running on host should use localhost:29092 when docker-compose maps that port to host. When you change advertised listeners to machine IP, update producer bootstrap server accordingly.

### 8 — Cleanup

**stop docker compose**\
docker compose down --volumes

**remove image (if needed)**\
docker rmi yourdockerhub/pirates-etl:1.0

**delete k8s resources**\
kubectl delete -f k8s/etl-deployment.yaml


### 9 — Tips & recommended improvements (next steps)

- Move credentials into Kubernetes Secrets or a secrets manager (AWS Secrets Manager, HashiCorp Vault).
- Add liveness/readiness probes and resource requests/limits in Deployment.
- Add health check endpoints to ETL and expose them in the K8s Deployment.
- Add logging and metrics (Prometheus + Grafana) for:
- consumer lag
- ETL error rate
- DB insert latency
- Use CI/CD to build & publish the ETL image automatically (GitHub Actions / GitLab CI).
- Use managed Kafka (MSK/Confluent) in production instead of local docker Kafka.
- For high-scale throughput, increase topic partitions and run ETL with multiple consumers in the same consumer group.

### Quick reference — important commands
**Docker compose**
- docker compose up -d
- docker compose ps
- docker compose down

**Build & push ETL image**
- docker build -t <registry>/pirates-etl:1.0 .
- docker push <registry>/pirates-etl:1.0

**Kubernetes**
- kubectl apply -f k8s/etl-deployment.yaml
- kubectl get pods
- kubectl logs -f deployment/etl
- kubectl delete -f k8s/etl-deployment.yaml








