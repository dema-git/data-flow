#!/bin/bash

echo "Checking model..."

python3 <<EOF
from huggingface_hub import hf_hub_download
import shutil
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TARGET_FILE = os.path.join(BASE_DIR, "meta-llama-3-8b-instruct.Q4_K_M.gguf")

if not os.path.exists(TARGET_FILE):

    print("Model was not found.\n", flush=True)
    print("Downloading model...", flush=True)

    cached_file = hf_hub_download(
        repo_id="noeljacob/Meta-Llama-3-8B-Instruct-Q4_K_M-GGUF",
        filename="meta-llama-3-8b-instruct.Q4_K_M.gguf",
        force_download=False
    )
    shutil.copy(cached_file, TARGET_FILE)
    print(f"Model downloaded to {TARGET_FILE}", flush=True)
else:
    print(f"Model already exists at {TARGET_FILE}", flush=True)
EOF

echo "Starting FastAPI..."
exec uvicorn main:app --host 0.0.0.0 --port 8100