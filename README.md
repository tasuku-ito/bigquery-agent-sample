# Code Samples

## 概要
BigQuery エージェントのサンプルコード集です。
2種類のエージェントが含まれており、それぞれ異なるアプローチで安全なクエリ実行を実現しています。

### エージェントの特徴
- **bq-agent-app (Callback方式)**
  - ツールの実行前後に自動的なチェック（Callback）を行います。
  - DELETE文などの危険な操作や、設定した閾値（デフォルト1GB）を超える高コストなクエリを自動的にブロックします。

- **bq-hitl-agent-app (Human-in-the-Loop方式)**
  - 実行前にプランを提示し、人間の承認を経てから実行します。
  - 高コストなクエリが生成された場合、ユーザーに承認を求め、許可が得られた場合のみ実行します。

## セットアップ方法

### uvでの初期化
プロジェクトのルートディレクトリで以下のコマンドを実行し、依存関係をインストールしてください。

```bash
uv sync
```

### 設定ファイルの修正
各ディレクトリ（`bq-agent-app/config.py`, `bq-hitl-agent-app/config.py`）内の設定を、ご自身の環境に合わせて変更してください。

- `project_id`: Google Cloud プロジェクトID
- `dataset_id`: BigQuery データセットID

## 使い方

### adk webでの利用方法
`uv run` を使用して、adk Webサーバーを起動します。

```bash
uv run adk web
```

