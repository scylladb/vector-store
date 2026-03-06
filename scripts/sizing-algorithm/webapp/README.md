# VSS Sizing Calculator — Web Application

A browser-based UI for the ScyllaDB Vector Search sizing algorithm
(`vss_sizing.py`).

## Prerequisites

* Python 3.10+

## Quick Start

```bash
# From the repository root:
cd scripts/sizing-algorithm/webapp

# Create a virtual environment (optional but recommended):
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies:
pip install -r requirements.txt

# Run the server:
gunicorn -b 127.0.0.1:5050 app:app
```

Open **http://127.0.0.1:5050** in your browser.

> **Tip:** For development with auto-reload, use:
> ```bash
> gunicorn -b 127.0.0.1:5050 --reload app:app
> ```
> Or fall back to the built-in Flask server:
> ```bash
> python app.py
> ```

## Project Structure

```
webapp/
├── app.py              # Flask server + /api/compute endpoint
├── requirements.txt    # Python dependencies
├── README.md           # This file
├── templates/
│   └── index.html      # Single-page HTML UI
└── static/
    ├── style.css       # Styles
    └── app.js          # Frontend logic (vanilla JS)
```

## How It Works

1. The HTML page presents sliders and controls for every sizing parameter
   (number of vectors, dimensions, QPS, recall, K, quantization, metadata
   size, filtering columns).
2. Results update automatically as you adjust any parameter (debounced at
   150 ms).
3. The frontend `POST`s the current values to `/api/compute`.
3. The Flask backend instantiates a `SizingInput`, calls
   `compute_sizing()`, and returns the full recommendation as JSON.
4. The frontend renders the results: instance selection, Vector Store-node RAM /
   vCPU requirements, HNSW parameters, ScyllaDB-node sizing, and estimated
   monthly cost.

## API Reference

### `POST /api/compute`

Runs the sizing algorithm and returns the full infrastructure recommendation.

#### Request

`Content-Type: application/json`

| Field | Type | Default | Description |
|---|---|---|---|
| `num_vectors` | int | 50 000 000 | Total vectors to index (10 000 – 4 000 000 000) |
| `dimensions` | int | 1 536 | Dimensions per vector (1 – 16 000) |
| `target_qps` | int | 1 000 | Target queries-per-second at P99 ≤ 15 ms (10 – 1 000 000) |
| `recall` | int | 95 | Target recall accuracy % (70 – 99) |
| `k` | int | 10 | Nearest-neighbour results per query (1 – 1 000) |
| `quantization` | string | `"none"` | `"none"`, `"scalar"`, or `"binary"` |
| `metadata_bytes_per_vector` | int | 100 | Avg metadata bytes per vector (4 – 1 048 576) |
| `filtering_columns` | int | 0 | Number of filtering columns (0 – 20) |
| `cloud_provider` | string | `"aws"` | `"aws"` or `"gcp"` |

All fields are optional; omitted fields use the defaults shown above.

**Example request:**

```json
{
  "num_vectors": 10000000,
  "dimensions": 768,
  "target_qps": 5000,
  "recall": 95,
  "k": 10,
  "quantization": "scalar",
  "metadata_bytes_per_vector": 256,
  "filtering_columns": 2,
  "cloud_provider": "aws"
}
```

#### Response (200 OK)

```json
{
  "hnsw": {
    "m": 32
  },
  "compression_ratio": 3.0,
  "vector_store_node": {
    "index_ram_gb": 12.34,
    "filtering_ram_gb": 0.56,
    "total_ram_gb": 12.9,
    "required_vcpus": 42,
    "base_qps_per_vcpu": 340.5,
    "effective_qps_per_vcpu": 47.67,
    "throughput_bucket": "medium-large"
  },
  "instance_selection": {
    "instance_type": "r7g.xlarge",
    "instance_vcpus": 4,
    "instance_ram_gb": 32,
    "num_instances": 12,
    "total_vcpus": 48,
    "total_ram_gb": 384.0,
    "total_cost_per_hour": 5.14,
    "total_cost_per_month": 3752.2,
    "total_cost_per_hour_yearly": 3.17,
    "total_cost_per_month_yearly": 2314.1
  },
  "scylladb_node": {
    "num_nodes": 3,
    "vcpus_per_node": 3,
    "total_vcpus": 9,
    "total_storage_gb": 31.14,
    "embedding_storage_gb": 28.75,
    "metadata_storage_gb": 2.38
  },
  "vector_store_replicas": 12,
  "summary": "=== ScyllaDB VSS Sizing Summary ===\n..."
}
```

#### Error Response (400 Bad Request)

Returned when input validation fails (out-of-range values, unknown
quantization/cloud provider, or when no instance has enough RAM).

```json
{
  "error": "num_vectors must be between 10,000 and 4,000,000,000"
}
```
