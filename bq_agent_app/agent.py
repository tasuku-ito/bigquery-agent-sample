from typing import Any, Dict, Optional

import google.auth
from google.adk.agents import Agent
from google.adk.tools.base_tool import BaseTool
from google.adk.tools.bigquery import BigQueryCredentialsConfig, BigQueryToolset
from google.adk.tools.bigquery.config import BigQueryToolConfig, WriteMode
from google.adk.tools.tool_context import ToolContext

from .config import config

# Agent Engineにデプロイするときにサービスアカウントでエージェントを動作させ
# Tool自体の認証にも同じサービスアカウントを使ってほしい場合は以下をコメントアウト
# application_default_credentials, _ = google.auth.default()
# credentials_config = BigQueryCredentialsConfig(
#     credentials=application_default_credentials
# )

tool_config = BigQueryToolConfig(write_mode=WriteMode.BLOCKED)

bigquery_toolset = BigQueryToolset(
    tool_filter=["execute_sql", "get_table_info", "list_table_ids", "list_dataset_ids"],
    # credentials_config=credentials_config, # Tool自体の認証にエージェントと同じサービスアカウントを使ってほしい場合はここをコメントアウト
    bigquery_tool_config=tool_config
)


async def update_bigquery_api_count(
    tool: BaseTool, args: Dict[str, Any], tool_context: ToolContext, tool_response: Dict
) -> None:
    if tool.name in ["execute_sql"]:
        if tool_response.get("status") == "ERROR":
            failure_count = tool_context.state.get("bigquery_api_failure", 0)
            tool_context.state["bigquery_api_failure"] = failure_count + 1
    return None


def validate_sql_callback(
    tool: BaseTool, args: Dict[str, Any], tool_context: ToolContext
) -> Optional[Dict]:
    if tool.name == "execute_sql":
        if "delete" in args.get("query", "").lower():
            return {
                "result": "Tool execution was blocked due to forbidden delete statement!"
            }
    print("@before_tool_callback: No blocking required")
    return None


def sql_query_dryrun_callback(
    tool: BaseTool, args: Dict[str, Any], tool_context: ToolContext
) -> Optional[Dict]:
    if tool.name == "execute_sql" and "query" in args and "project_id" in args:
        query = args.get("query")
        project_id = args.get("project_id")
        from google.cloud import bigquery

        # Construct a BigQuery client object.
        client = bigquery.Client()

        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)

        # Start the query, passing in the extra configuration.
        query_job = client.query(
            query,
            project=project_id,
            job_config=job_config,
        )  # Make an API request.

        total_bytes_processed = query_job.total_bytes_processed

        # A dry run query completes immediately.
        print("This query will process {} bytes.".format(total_bytes_processed))
        if total_bytes_processed > config.dry_run_threshold_bytes:
            threshold_gb = config.dry_run_threshold_bytes / 1_000_000_000
            return {
                "result": f"Large size SQL query is forbidden. The query size by dry run is {total_bytes_processed} bytes > {threshold_gb} GB"
            }
        else:
            print("@sql_query_dryrun_callback: SQL query dry run successful")
            return None
    print("@sql_query_dryrun_callback: No dry run required")
    return None


root_agent = Agent(
    model=config.model,
    name="bigquery_agent_eval",
    description=("BigQuery データに対する質問に SQL を実行して回答するエージェント"),
    instruction=f"""
    あなたは BigQuery ツールにアクセスできるデータ分析エージェントです。
    適切なツールを用いて BigQuery のメタデータを取得したり SQL クエリを実行し、ユーザーの質問に日本語で回答してください。
    すべてのクエリは project-id: {config.project_id} 上で実行してください。
    テーブルはデータセット: {config.dataset_id}に存在します。
    Callback の結果からクエリの実行が禁止されている場合は、その理由をユーザーに報告し、このタスクを終了してください。
""",
    tools=[bigquery_toolset],
    after_tool_callback=update_bigquery_api_count,
    before_tool_callback=[validate_sql_callback, sql_query_dryrun_callback],
)
