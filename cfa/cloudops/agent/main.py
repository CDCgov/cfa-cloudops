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


def load_plan(goal):
    if "hello" in goal:
        file_name = "workflow_1.yaml"
    elif "measles" in goal:
        file_name = "workflow_2.yaml"
    else:
        file_name = "workflow_3.yaml"
    with open(f"output/{file_name}", "r") as f:
        plan_yaml = f.read()
    return plan_yaml


def run_plan(workflow: str):
    client = CloudClient(dotenv_path="metaflow.env", use_sp=True)
    client.cred.azure_resource_group_name = "EXT-EDAV-CFA-PRD"
    executor = WorkflowExecutor(
        client=client,
        dependency_yaml_path="config/operational_dependencies.yaml",
        dry_run=False,
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
    # workflow = create_plan(text)
    workflow = load_plan(text)
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
