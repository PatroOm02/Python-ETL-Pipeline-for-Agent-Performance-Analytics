# Python-ETL-Pipeline-for-Agent-Performance-Analytics
# 📞  Agent Performance Pipeline

This project implements a simplified end-to-end data pipeline to process, validate, and analyze daily loan collection call data. It merges inputs from multiple CSV sources and generates a performance summary per agent, including Slack notifications and persistent logging.

---

## 🧠 Objective

Automate the transformation and analysis of daily call logs, agent rosters, and disposition data to compute key metrics like:
- ✅ Total Calls
- 📈 Connect Rate
- ⏱ Average Duration
- 👤 Agent Presence

---

## 📂 Project Structure

![image](https://github.com/user-attachments/assets/bb98af00-59eb-45c4-b2ea-3503b649d07b)


---

## ⚙️ Features

- ✅ Validates data schema, flags duplicates & missing fields
- 🔄 Merges datasets across `agent_id`, `org_id`, and `call_date`
- 📊 Computes per-agent KPIs (connect rate, unique loans, duration)
- 🧾 Outputs daily summary to CSV
- 🧵 Logs pipeline runs (console + persistent file logs)
- 🔔 Sends formatted Slack notifications using Incoming Webhooks
- 🧪 Supports command-line arguments for flexible usage

---


🧰 Tech Stack
Language: Python

Libraries: pandas, argparse, requests, logging

Notifications: Slack Incoming Webhooks

Environment: PyCharm + venv (recommended)

📌 Example Slack Output
Agent Summary for 2025-04-28
*Top Performer*: Ravi Sharma (98% connect rate)
*Total Active Agents*: 45
*Average Duration*: 6.5 min
✅ Future Improvements
Unit tests for validation functions

Support for scheduling via Task Scheduler or cron

Visual dashboards for better analytics

🧑‍💻 Author
Om Patro
Final Year IT Student, VIT Vellore
