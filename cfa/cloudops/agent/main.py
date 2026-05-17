from pathlib import Path

import yaml
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from workflow_executor import WorkflowExecutor
from workflow_planner import generate_workflow

from cfa.cloudops import CloudClient

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


class TextRequest(BaseModel):
    text: str


def create_plan(goal):
    plan = generate_workflow(goal)
    return yaml.safe_dump(plan.model_dump(), sort_keys=False)
    # goal_id = secrets.token_hex(4)
    # workflow = yaml.safe_dump(plan.model_dump(), sort_keys=False)
    # with open(f"output/generated_workflow_{goal_id}.yaml", "w") as f:
    #    f.write(workflow)
    # return goal_id, workflow


def run_plan(workflow: str):
    client = CloudClient(dotenv_path="metaflow.env", use_sp=True)
    client.cred.azure_resource_group_name = "EXT-EDAV-CFA-PRD"
    executor = WorkflowExecutor(
        client=client,
        dependency_yaml_path="config/operational_dependencies.yaml",
        dry_run=True,
    )
    _, log_entries = executor.execute_plan(workflow)
    return "\n".join(log_entries)


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse(
        "index.html",  # MUST be first argument
        {"request": request},  # MUST include request
    )


@app.post("/plan", response_class=HTMLResponse)
async def plan(request: Request, text: str = Form(...)):
    workflow = create_plan(text)
    return templates.TemplateResponse(
        "index.html",  # template name FIRST
        {"request": request, "workflow": workflow},
    )


@app.post("/execute", response_class=HTMLResponse)
async def execute(request: Request, workflow_yaml: str = Form(...)):
    workflow_status = run_plan(workflow_yaml)
    print(workflow_status)
    return templates.TemplateResponse(
        "index.html",  # template name FIRST
        {"request": request, "workflow_status": workflow_status},
    )
