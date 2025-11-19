# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
from dataclasses import dataclass

import google.auth

# To use AI Studio credentials:
# 1. Create a .env file in the /app directory with:
#    GOOGLE_GENAI_USE_VERTEXAI=FALSE
#    GOOGLE_API_KEY=PASTE_YOUR_ACTUAL_API_KEY_HERE
# 2. This will override the default Vertex AI configuration
_, project_id = google.auth.default()
os.environ.setdefault("GOOGLE_CLOUD_PROJECT", project_id)
os.environ.setdefault("GOOGLE_CLOUD_LOCATION", "global")
os.environ.setdefault("GOOGLE_GENAI_USE_VERTEXAI", "True")


@dataclass
class BigQueryAgentConfiguration:
    """Configuration for BigQuery agent.

    Attributes:
        model (str): Model for query generation and execution tasks.
        project_id (str): Google Cloud project ID for BigQuery.
        dataset_id (str): BigQuery dataset ID.
        dry_run_threshold_bytes (int): Byte threshold for blocking query execution via dry-run (default: 1GB).
    """

    model: str = "gemini-2.5-flash"
    project_id: str = "your-project-id"
    dataset_id: str = "your-dataset-id"
    dry_run_threshold_bytes: int = 1_000_000_000  # 1GB


config = BigQueryAgentConfiguration()
