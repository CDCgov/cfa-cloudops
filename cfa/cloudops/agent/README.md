Do not let the agent directly call delete_job, delete_pool, or production-changing methods without validation. The safer pattern is:

LLM goal → structured YAML → dependency validation → dry-run preview → approved execution.
