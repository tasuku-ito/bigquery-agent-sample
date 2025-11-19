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

import logging
from typing import Any, Dict

import google.auth
from google.adk.agents import LlmAgent, SequentialAgent
from google.adk.tools import FunctionTool
from google.adk.tools.agent_tool import AgentTool
from google.adk.tools.bigquery import BigQueryCredentialsConfig, BigQueryToolset
from google.adk.tools.bigquery.config import BigQueryToolConfig, WriteMode
from google.adk.tools.tool_context import ToolContext

from .config import config

application_default_credentials, _ = google.auth.default()
credentials_config = BigQueryCredentialsConfig(
    credentials=application_default_credentials
)

tool_config = BigQueryToolConfig(write_mode=WriteMode.BLOCKED)

# BigQuery Toolset
bigquery_toolset = BigQueryToolset(
    tool_filter=["execute_sql", "get_table_info", "list_table_ids", "list_dataset_ids"],
    credentials_config=credentials_config,
    bigquery_tool_config=tool_config
)


async def check_query_cost(
    tool_context: ToolContext, query: str, project_id: str
) -> Dict[str, Any]:
    """BigQueryクエリのドライランを実行し、スキャン量を確認する。

    Args:
        tool_context: Tool context containing session state.
        query: SQL query string to check.
        project_id: Google Cloud project ID.

    Returns:
        Dictionary containing:
        - status: "APPROVAL_REQUIRED" if bytes exceed threshold, "APPROVED" otherwise
        - total_bytes_processed: Number of bytes that would be scanned
        - message: Human-readable message about the scan cost
    """
    if not query or not project_id:
        return {
            "status": "ERROR",
            "message": "Query or Project ID is missing",
        }

    try:
        from google.cloud import bigquery

        # BigQueryクライアントの初期化とドライラン実行
        client = bigquery.Client()
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        query_job = client.query(
            query,
            project=project_id,
            job_config=job_config,
        )

        total_bytes_processed = query_job.total_bytes_processed

        # 承認が必要な閾値を超えているかチェック
        if total_bytes_processed >= config.approval_threshold_bytes:
            # スキャン量とクエリをセッションステートに保存
            tool_context.state["pending_query"] = query
            tool_context.state["pending_query_bytes"] = total_bytes_processed
            tool_context.state["pending_query_project_id"] = project_id

            bytes_gb = total_bytes_processed / 1_000_000_000
            return {
                "status": "APPROVAL_REQUIRED",
                "total_bytes_processed": total_bytes_processed,
                "message": f"このクエリは約 {bytes_gb:.2f} GB ({total_bytes_processed:,} bytes) のデータをスキャンします。実行するには承認が必要です。",
            }
        else:
            bytes_mb = total_bytes_processed / 1_000_000
            return {
                "status": "APPROVED",
                "total_bytes_processed": total_bytes_processed,
                "message": f"このクエリは約 {bytes_mb:.2f} MB ({total_bytes_processed:,} bytes) のデータをスキャンします。承認不要で実行できます。",
            }

    except Exception as e:
        logging.error(f"Error during query dry-run: {e}")
        return {
            "status": "ERROR",
            "message": f"クエリのドライラン中にエラーが発生しました: {str(e)}",
        }


check_query_cost_tool = FunctionTool(func=check_query_cost)


# --- Query Plan Generator Agent ---
query_plan_generator = LlmAgent(
    model=config.worker_model,
    name="query_plan_generator",
    description="ユーザーの質問に基づいてBigQueryのメタデータを使用してSQLクエリプランを生成します。",
    instruction=f"""
    あなたはBigQueryのSQLクエリプランナーです。ユーザーの質問に基づいてSQLクエリプランを作成するのがあなたの仕事です。

    **ワークフロー:**
    1. BigQueryツールセットを使用して、データセット内の利用可能なテーブルとスキーマを探索します。
    2. ユーザーの質問に答える適切なSQLクエリを生成します。
    3. クエリを生成した後、必ず`check_query_cost`ツールを呼び出してスキャンコストを確認します。
    4. クエリとコスト情報をユーザーに提示します。

    **重要なルール:**
    - 常にproject_id: {config.project_id}を使用してください。
    - 常にdataset: {config.dataset_id}を使用してください。
    - クエリを生成した後、続行する前に必ず`check_query_cost`を呼び出してください。
    - ステータスが"APPROVAL_REQUIRED"の場合、クエリとコスト情報をユーザーに提示し、明示的な承認を待ちます。
    - ステータスが"APPROVED"の場合、実行に進むことができます。
    - ステータスが"ERROR"の場合、エラーをユーザーに説明します。

    **出力形式:**
    クエリプランを提示する際は、以下を含めてください：
    - SQLクエリ
    - 推定スキャンコスト
    - 承認が必要かどうか
    - 承認が必要な場合の明確な承認リクエスト（例：「このクエリを実行するには承認が必要です。承認する場合は「承認します」または「実行してください」とお答えください。」）
    """,
    tools=[bigquery_toolset, check_query_cost_tool],
    output_key="query_plan",
)


# --- Query Executor Agent ---
query_executor_agent = LlmAgent(
    model=config.worker_model,
    name="query_executor",
    description="承認されたBigQuery SQLクエリを実行し、結果を分析します。",
    instruction=f"""
    あなたはBigQueryクエリ実行者兼データアナリストです。承認されたSQLクエリを実行し、ユーザーに洞察を提供するのがあなたの仕事です。

    **ワークフロー:**
    1. セッションステート（`approved_query`キー）から承認されたSQLクエリを取得します。このクエリは既にユーザーによって承認されています。
    2. project_id: {config.project_id}とdataset: {config.dataset_id}を使用してBigQueryツールセットでクエリを実行します。
    3. 結果を分析し、ユーザーの元の質問に対して明確で洞察に富んだ回答を日本語で提供します。

    **重要なルール:**
    - セッションステート（`approved_query`）のクエリは既に承認されているため、直接実行できます。
    - project_id: {config.project_id}を使用してください。
    - dataset: {config.dataset_id}を使用してください。
    - 結果を明確でユーザーフレンドリーな形式で提示します。
    - クエリが失敗した場合、エラーを明確に説明し、代替案を提案します。

    **出力:**
    - クエリ結果に基づいてユーザーの質問に対する明確な回答を提供します。
    - 関連するデータポイントと洞察を含めます。
    - 結果を読みやすい形式（テーブル、要約など）でフォーマットします。
    """,
    tools=[bigquery_toolset],
)


# --- Interactive Planner Agent (HITL) ---
interactive_planner_agent = LlmAgent(
    name="interactive_planner_agent",
    model=config.worker_model,
    description="主要なBigQueryアシスタント。実行前にユーザーと協力してクエリプランを作成し、承認を得ます。",
    instruction=f"""
    あなたはBigQueryクエリ計画アシスタントです。あなたの主な機能は、ユーザーの要求を承認されたSQLクエリプランに変換することです。

    **重要なルール: 高コストのクエリについては、ユーザーの承認なしに直接クエリを実行しないでください。**

    あなたのワークフローは以下の通りです：
    1. **計画:** `query_plan_generator`ツールを使用して、ユーザーの質問に基づいてSQLクエリプランを作成します。
    2. **提示:** 生成されたクエリとその推定スキャンコストをユーザーに表示します。
    3. **承認待ち:** 
       - 承認が必要な場合（スキャンコスト >= {config.approval_threshold_bytes / 1_000_000_000} GB）、ユーザーに明示的に承認を求めます。
       - クエリをセッションステートに`pending_query`として保存します。
       - ユーザーの明示的な確認を待ちます（例：「承認します」「実行してください」「OK」「承認」「了解」「はい」）。
    4. **実行:** ユーザーが明示的に承認した場合、または承認が不要な場合：
       - セッションステートの`approved_query`を保留中のクエリ（または承認が不要だった場合は生成されたクエリ）に設定します。
       - タスクを`query_executor`エージェントに委譲します。

    **承認処理:**
    - クエリに承認が必要な場合、続行する前にユーザーの明示的な確認を待つ必要があります。
    - ユーザーメッセージ内の承認キーワードを探します：「承認」「承認します」「実行」「実行してください」「OK」「了解」「はい」
    - ユーザーから明確な承認を受けるまで実行に進まないでください。
    - ユーザーが拒否したり、クエリの変更を求めたりした場合、`query_plan_generator`を再度使用してプランを改善します。
    - 承認されたら、`approved_query` = `pending_query`を設定し、`query_executor`に制御を渡します。

    **クエリストレージ:**
    - 保留中のクエリ: `pending_query`, `pending_query_bytes`, `pending_query_project_id`
    - 承認されたクエリ: `approved_query`, `approved_query_project_id`
    - 実行エージェントに委譲する際は、承認されたクエリの値を使用します。

    現在のプロジェクト: {config.project_id}
    現在のデータセット: {config.dataset_id}
    """,
    sub_agents=[query_executor_agent],
    tools=[AgentTool(query_plan_generator)],
    output_key="query_plan",
)


# --- Root Agent (Sequential Workflow) ---
root_agent = SequentialAgent(
    name="BigQueryHITLWorkflow",
    description="高コストクエリに対して人間による承認を含むBigQueryクエリ計画と実行のワークフロー。",
    sub_agents=[
        interactive_planner_agent,
    ],
)
