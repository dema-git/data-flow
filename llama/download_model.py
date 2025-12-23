###########################################################################
# download_model.py
#
# Download and prepare the Meta-LLaMA 3 8B Instruct model.
#
# This script performs the following tasks:
# 1. Defines the local path to store the model (`model.gguf` in the same directory as this script).
# 2. Checks if the model already exists locally.
# 3. If the model is missing, downloads it from Hugging Face Hub using `hf_hub_download`.
# 4. Moves/renames the downloaded file to the target path if necessary.
# 5. Ensures the target directory exists.
#
# Notes:
# - The model is downloaded only once; subsequent runs will skip the download.
# - The script is designed to work with the GGUF format of the Meta-LLaMA 3 8B Instruct model.
###########################################################################

from huggingface_hub import hf_hub_download
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MODEL_PATH = os.path.join(BASE_DIR, "model.gguf")

REPO_ID = "noeljacob/Meta-Llama-3-8B-Instruct-Q4_K_M-GGUF"
FILENAME = "meta-llama-3-8b-instruct.Q4_K_M.gguf"

os.makedirs(BASE_DIR, exist_ok=True)

if not os.path.exists(MODEL_PATH):
    print("Model not found. Downloading…")
    downloaded_file = hf_hub_download(
        repo_id=REPO_ID,
        filename=FILENAME,
        cache_dir=BASE_DIR,
        force_download=False
    )

    if downloaded_file != MODEL_PATH:
        os.rename(downloaded_file, MODEL_PATH)
    print(f"Model has been saved  {MODEL_PATH}")
else:
    print("The model already exists. Download skipped.")