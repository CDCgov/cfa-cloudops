Do not let the agent directly call delete_job, delete_pool, or production-changing methods without validation. The safer pattern is:

LLM goal → structured YAML → dependency validation → dry-run preview → approved execution.


Web site setup

pip install -r requirements-web.txt
uvicorn main:app --host 0.0.0.0 --port 9000 --reload
