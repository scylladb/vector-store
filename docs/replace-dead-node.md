# Replace a Dead Vector Store Node

The replace operation provisions a new Vector Store node in the same availability zone as the dead node, waits for it to fetch all data from ScyllaDB and rebuild its indexes, then decommissions the old node. This is an **add-then-remove** strategy that ensures zero downtime for vector search queries (assuming other VS nodes in the cluster remain healthy).

The duration of this procedure depends on the data volume and index complexity. Index rebuilding can take anywhere from a few minutes to over an hour.

## Prerequisites

### Healthy ScyllaDB Cluster

The ScyllaDB cluster must be healthy and reachable. The new Vector Store node needs to connect to a ScyllaDB node via CQL to fetch data and build indexes.

### Identify the Dead Node

Collect the following information from the dead Vector Store node (or from your infrastructure records):

- **Private IP address** of the dead node
- **Availability zone / rack** the node was running in
- **Instance type** (e.g., `i3.xlarge`)
- **Vector Store version** (the version of the `vector-store` binary or AMI)

### Collect Configuration from a Healthy Vector Store Node

If you have other healthy Vector Store nodes in the cluster, SSH into one and collect:

```bash
# View the current configuration
cat /home/ubuntu/.env
```

This will show the environment variables used by the Vector Store service, including the CQL username, password file path, ScyllaDB connection URI, and TLS settings. You will replicate this configuration (with appropriate IP changes) on the replacement node.

### Collect CQL Credentials

You need the CQL username and password that Vector Store uses to connect to ScyllaDB. On a healthy VS node:

```bash
# Username is in the .env file
grep VECTOR_STORE_SCYLLADB_USERNAME /home/ubuntu/.env

# Password is stored on a tmpfs mount
sudo cat /mnt/vector_store/.vector_store_password.txt
```

## Procedure

### Step 1: Provision a New Instance

Launch a new VM using the same Vector Store AMI/image as the dead node:

- **Same instance type** as the dead node
- **Same availability zone** (rack) as the dead node
- **Same VPC and subnet** as the rest of the cluster
- Enable **root volume encryption**

> **Note:** The instance name convention used by ScyllaDB Cloud is
> `Scylla-Cloud-<CLUSTER_ID>-<DC_NAME>-VectorSearch-<NODE_ID>` on AWS, and
> `scylla-cloud-<cluster_id>-vectorsearch-<node_id>-<checksum>` on GCP.
> You can use any naming convention that fits your environment.

### Step 2: Configure Firewall Rules

The following network access is required:

| Direction | From | To | Port | Purpose |
|-----------|------|----|------|---------|
| Inbound | ScyllaDB nodes | New VS node | TCP 6080 | ScyllaDB connects to VS for vector search |
| Inbound | Prometheus / monitoring | New VS node | TCP 6080 | Metrics scraping (`/metrics` endpoint) |
| Outbound | New VS node | A ScyllaDB node in the same rack | TCP 9042 | CQL native transport |
| Outbound | New VS node | A ScyllaDB node in the same rack | TCP 9142 | CQL native transport (SSL), if encryption is enabled |

On AWS, update the relevant security groups. On GCP, update the firewall rules.

### Step 3: Configure the Vector Store Node

SSH into the new node and perform the following steps.

#### 3a. Create a tmpfs Mount for the Password File

The CQL password must be stored in memory only (tmpfs) for security:

```bash
sudo mkdir -p /mnt/vector_store
sudo mount -t tmpfs -o size=10m tmpfs /mnt/vector_store
```

#### 3b. Write the CQL Password File

```bash
# Replace <PASSWORD> with the actual CQL password collected earlier
echo -n '<PASSWORD>' | sudo tee /mnt/vector_store/.vector_store_password.txt > /dev/null
sudo chown ubuntu:ubuntu /mnt/vector_store/.vector_store_password.txt
sudo chmod 600 /mnt/vector_store/.vector_store_password.txt
```

#### 3c. Create the `.env` Configuration File

Create `/home/ubuntu/.env` with the following content. Replace the placeholder values with your actual configuration:

**Without client-to-node encryption:**

```bash
cat > /home/ubuntu/.env << 'EOF'
export VECTOR_STORE_SCYLLADB_URI=<SCYLLA_PRIVATE_IP>:9042
export VECTOR_STORE_URI=0.0.0.0:6080
export VECTOR_STORE_SCYLLADB_USERNAME=<CQL_USERNAME>
export VECTOR_STORE_SCYLLADB_PASSWORD_FILE=/mnt/vector_store/.vector_store_password.txt
EOF
chmod 600 /home/ubuntu/.env
chown ubuntu:ubuntu /home/ubuntu/.env
```

**With client-to-node encryption (SSL CQL):**

```bash
cat > /home/ubuntu/.env << 'EOF'
export VECTOR_STORE_SCYLLADB_URI=<SCYLLA_PRIVATE_IP>:9142
export VECTOR_STORE_URI=0.0.0.0:6080
export VECTOR_STORE_SCYLLADB_USERNAME=<CQL_USERNAME>
export VECTOR_STORE_SCYLLADB_PASSWORD_FILE=/mnt/vector_store/.vector_store_password.txt
export VECTOR_STORE_SCYLLADB_CERTIFICATE_FILE=/home/ubuntu/vector_store_client.pem
EOF
chmod 600 /home/ubuntu/.env
chown ubuntu:ubuntu /home/ubuntu/.env
```

If your deployment also uses HTTPS for the Vector Store API (ScyllaDB-to-VS communication), add these lines:

```
export VECTOR_STORE_TLS_CERT_PATH=/home/ubuntu/server_cert.pem
export VECTOR_STORE_TLS_KEY_PATH=/home/ubuntu/server_key.pem
```

And provision the corresponding TLS certificate and key files (signed by the same CA used for client-to-node encryption).

> **Important:** `VECTOR_STORE_SCYLLADB_URI` should point to the private IP of
> a **single ScyllaDB node in the same rack/AZ** as the Vector Store node.
> The Vector Store connects to one ScyllaDB node, not the entire cluster.

#### 3d. Provision TLS Certificates (if applicable)

If client-to-node encryption is enabled, copy the CA certificate to the new VS node:

```bash
# Copy the CA certificate used for client-to-node encryption
scp ca-cert.pem ubuntu@<NEW_VS_NODE_IP>:/home/ubuntu/vector_store_client.pem
ssh ubuntu@<NEW_VS_NODE_IP> 'chmod 600 /home/ubuntu/vector_store_client.pem'
```

If HTTPS is enabled for the VS API, generate and provision a server certificate signed by the same CA:

```bash
# Generate a server certificate for the VS node's IP
# (use your CA to sign it, with the VS node's private IP as the CN/SAN)
scp server_cert.pem ubuntu@<NEW_VS_NODE_IP>:/home/ubuntu/server_cert.pem
scp server_key.pem ubuntu@<NEW_VS_NODE_IP>:/home/ubuntu/server_key.pem
ssh ubuntu@<NEW_VS_NODE_IP> 'chmod 600 /home/ubuntu/server_cert.pem /home/ubuntu/server_key.pem'
```

If HTTPS is enabled, you must also ensure the CA certificate (truststore) is configured on each ScyllaDB node so that ScyllaDB can verify the VS node's TLS certificate. If this is already set up for the existing VS nodes, no action is needed. If not, upload the CA cert to each ScyllaDB node and configure `vector_store_encryption_options` in `scylla.yaml` — this requires a ScyllaDB restart.

### Step 4: Start the Vector Store Service

```bash
sudo systemctl start vector-store
```

Verify the service is running:

```bash
sudo systemctl status vector-store
```

The service should show as `active (running)`. If it fails to start, check the logs:

```bash
sudo journalctl -u vector-store -f
```

### Step 5: Wait for the Node to Become Ready

After starting, the Vector Store node fetches data from ScyllaDB and builds its indexes. Poll the status endpoint until it returns `SERVING`:

```bash
# Without TLS:
watch -n 30 'curl -s http://localhost:6080/api/v1/status'

# With TLS:
watch -n 30 'curl -sk https://localhost:6080/api/v1/status'
```

The endpoint returns a plain-text string:
- **While initializing:** a status other than `SERVING` (or a connection error)
- **When ready:** `SERVING`

This can take **up to 1 hour or more** depending on the data volume and number of indexes. Do not proceed to the next step until the status is `SERVING`.

### Step 6: Update ScyllaDB Configuration

Once the new VS node is ready, update the ScyllaDB nodes so they know about the replacement.

On **each ScyllaDB node in the same datacenter**, update `scylla.yaml` with the new Vector Store URIs:

- `vector_store_primary_uri` — Comma-separated list of Vector Store node URIs in the **same rack** as the ScyllaDB node. Example: `http://10.0.1.5:6080` (or `https://...` if TLS is enabled).
- `vector_store_secondary_uri` — Comma-separated list of Vector Store node URIs in **other racks** within the same datacenter.

For example, if you have 3 racks (A, B, C) with one VS node each, a ScyllaDB node in rack A would have:

```yaml
vector_store_primary_uri: "http://10.0.1.5:6080"         # VS node in rack A
vector_store_secondary_uri: "http://10.0.2.5:6080,http://10.0.3.5:6080"  # VS nodes in racks B and C
```

Replace the dead node's IP with the new node's IP in these URIs.

After updating `scylla.yaml`, trigger a configuration reload. Setting `vector_store_primary_uri` triggers an automatic reload in ScyllaDB — alternatively, you can reload explicitly. **No ScyllaDB restart is required** for URI changes (a restart is only required if you are setting up `vector_store_encryption_options` for the first time).

### Step 7: Decommission the Dead Node

1. **Terminate the old VM instance** (or shut it down and delete it).
2. **Remove the dead node's IP** from any security group or firewall rules that referenced it specifically.
3. **Remove from monitoring** — delete the old node's scrape target from Prometheus configuration.

### Step 8: Update Monitoring

Add the new Vector Store node to your Prometheus scrape configuration:

```yaml
- targets:
  - '<NEW_VS_NODE_IP>:6080'
```

Reload Prometheus and verify metrics are being collected from the new node.

## Verification

After completing the procedure, verify the replacement was successful:

1. **Vector Store status** — The new node's status endpoint returns `SERVING`:
   ```bash
   curl -s http://<NEW_VS_NODE_IP>:6080/api/v1/status
   # Expected: SERVING
   ```

2. **Vector search queries** — Run a vector search query against your ScyllaDB cluster and verify results are correct.

3. **Monitoring** — Check that the new VS node appears in Prometheus/Grafana with `UP = 1`, and that `IndexCount` and other metrics match expectations.

4. **Old node removed** — Confirm the dead node's IP no longer appears in any `scylla.yaml` configuration, security group rules, or monitoring targets.

## Operational Notes

### Hot Configuration Reload

The Vector Store service supports hot configuration reload via `SIGHUP`. If you need to change the `.env` file after the service is running (e.g., to update the ScyllaDB IP or credentials), edit the file and then:

```bash
sudo systemctl kill -s SIGHUP vector-store
```

### ScyllaDB Node Failure

Each Vector Store node is configured to connect to a **single ScyllaDB node** in the same rack. If that ScyllaDB node fails, the Vector Store node's configuration must be updated to point to a different ScyllaDB node in the same rack:

1. Edit `/home/ubuntu/.env` and change the `VECTOR_STORE_SCYLLADB_URI` to the IP of another healthy ScyllaDB node in the same rack.
2. Send SIGHUP: `sudo systemctl kill -s SIGHUP vector-store`

### Port Reference

| Port | Service | Description |
|------|---------|-------------|
| 6080 | Vector Store | API, status, and Prometheus metrics endpoint |
| 9042 | ScyllaDB | CQL native transport |
| 9142 | ScyllaDB | CQL native transport (SSL) |

### File Locations on a Vector Store Node

| Path | Description |
|------|-------------|
| `/home/ubuntu/.env` | Vector Store configuration (environment variables) |
| `/mnt/vector_store/.vector_store_password.txt` | CQL password (on tmpfs, in-memory only) |
| `/home/ubuntu/vector_store_client.pem` | CA certificate for CQL SSL (if encryption enabled) |
| `/home/ubuntu/server_cert.pem` | VS API TLS certificate (if HTTPS enabled) |
| `/home/ubuntu/server_key.pem` | VS API TLS private key (if HTTPS enabled) |

### Environment Variables Reference

| Variable | Required | Description |
|----------|----------|-------------|
| `VECTOR_STORE_SCYLLADB_URI` | Yes | ScyllaDB CQL endpoint (`<IP>:<PORT>`) |
| `VECTOR_STORE_URI` | Yes | Listen address (`0.0.0.0:6080`) |
| `VECTOR_STORE_SCYLLADB_USERNAME` | Yes | CQL username |
| `VECTOR_STORE_SCYLLADB_PASSWORD_FILE` | Yes | Path to CQL password file |
| `VECTOR_STORE_SCYLLADB_CERTIFICATE_FILE` | No | CA cert for CQL SSL connections |
| `VECTOR_STORE_TLS_CERT_PATH` | No | TLS cert for HTTPS API |
| `VECTOR_STORE_TLS_KEY_PATH` | No | TLS key for HTTPS API |
