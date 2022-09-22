# files-airflow

Meltano project [file bundle](https://docs.meltano.com/concepts/plugins#file-bundles) for [Airflow](https://airflow.apache.org/).

Files:
- [`orchestrate/dags/meltano.py`](./bundle/orchestrate/dags/meltano.py)

```py
# Add Airflow orchestrator and this file bundle to your Meltano project
meltano add orchestrator airflow

# Add only this file bundle to your Meltano project
meltano add files airflow
```
