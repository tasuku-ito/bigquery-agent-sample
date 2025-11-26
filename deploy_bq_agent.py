import asyncio
import vertexai
from bq_agent_app.agent import root_agent # modify this if your agent is not in agent.py
from vertexai import agent_engines
from bq_agent_app.config import config

PROJECT_ID = config.project_id
LOCATION = "us-central1"
STAGING_BUCKET = f"gs://{PROJECT_ID}-staging"
SERVICE_ACCOUNT = f"bq-agent-sample@{PROJECT_ID}.iam.gserviceaccount.com"

# Initialize the Vertex AI SDK
vertexai.init(
    project=PROJECT_ID,
    location=LOCATION,
    staging_bucket=STAGING_BUCKET,
)

# Wrap the agent in an AdkApp object
app = agent_engines.AdkApp(
    agent=root_agent,
    enable_tracing=True,
)

def deploy_agent():
    remote_app = agent_engines.create(
        agent_engine=app,
        display_name=root_agent.name,
        requirements=[
            "google-adk==1.19.0",
            "google-cloud-aiplatform[adk,agent_engines]==1.128.0",
            'pydantic==2.12.4',
            'cloudpickle==3.1.2',
        ],
        extra_packages=["bq_agent_app"],
        service_account=SERVICE_ACCOUNT,
    )

    print("Deployment finished!")
    print(f"Resource Name: {remote_app.resource_name}")

async def test_run():
# Create a local session to maintain conversation history
    session = await app.async_create_session(user_id="u_123")
    print(session)
    events = []
    async for event in app.async_stream_query(
        user_id="u_123",
        session_id=session.id,
        message="BigQueryのテーブル一覧を教えてください",
    ):
        events.append(event)

    # The full event stream shows the agent's thought process
    print("--- Full Event Stream ---")
    for event in events:
        print(event)

    # For quick tests, you can extract just the final text response
    final_text_responses = [
        e for e in events
        if e.get("content", {}).get("parts", [{}])[0].get("text")
        and not e.get("content", {}).get("parts", [{}])[0].get("function_call")
    ]
    if final_text_responses:
        print("\n--- Final Response ---")
        print(final_text_responses[0]["content"]["parts"][0]["text"])


if __name__ == "__main__":
    # asyncio.run(test_run())
    deploy_agent()
