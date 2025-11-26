import asyncio
import vertexai

from bq_agent_app.config import config

PROJECT_ID = config.project_id
LOCATION = "us-central1"

async def main():
    client = vertexai.Client(
        project=PROJECT_ID,
        location=LOCATION,
    )

    remote_app = client.agent_engines.get(name=f"projects/{PROJECT_ID}/locations/{LOCATION}/reasoningEngines/xxxxxxxxxx")


    session = await remote_app.async_create_session(user_id="u_456")

    async for event in remote_app.async_stream_query(
        user_id="u_456",
        session_id=session["id"],
        message="wikidata テーブルで日本の記事が何本あるか確認してください。",
    ):
        print(event)


if __name__ == "__main__":
    asyncio.run(main())