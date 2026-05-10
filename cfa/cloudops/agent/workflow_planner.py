from typing import Literal, Union

from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, ConfigDict, Field


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


# class TimedeltaSpec(StrictModel):
#    days: int | None = None
#    hours: int | None = None
#    minutes: int | None = None


class AddTaskArgs(StrictModel):
    job_name: str
    command_line: str
    #    mount_pairs: list[dict] | None = None
    name_suffix: str = ""
    depends_on: str | None = None
    #    depends_on_range: tuple | None = None
    run_dependent_tasks_on_fail: bool = False
    container_image_name: str = None


class CheckJobStatusArgs(StrictModel):
    job_name: str


class CreatePoolArgs(StrictModel):
    pool_name: str
    mounts: list[dict[str, str]] | None = None
    container_image_name: str
    autoscale: bool = True
    max_autoscale_nodes: int = 3


class CreateJobArgs(StrictModel):
    job_name: str
    pool_name: str
    save_logs_to_blob: str | None = None
    logs_folder: str | None = None


# class CreateJobScheduleArgs(StrictModel):
#    job_schedule_name: str
#    pool_name: str
#    command: str
#    timeout: int = 30
#    start_window: TimedeltaSpec = None,
#    recurrence_interval: TimedeltaSpec = None


class DeleteJobArgs(StrictModel):
    job_name: str


class DeletePoolArgs(StrictModel):
    pool_name: str


class MonitorJobArgs(StrictModel):
    job_name: str


class CreateBlobContainerArgs(StrictModel):
    job_name: str


class UploadFilesArgs(StrictModel):
    files: str | list[str]
    container_name: str
    local_root_dir: str = "."
    location_in_blob: str = "."


class AddTaskStep(StrictModel):
    id: str
    method: Literal["add_task"]
    depends_on: list[str] = Field(default_factory=list)
    args: AddTaskArgs


class CheckJobStatusStep(StrictModel):
    id: str
    method: Literal["check_job_status"]
    depends_on: list[str] = Field(default_factory=list)
    args: CheckJobStatusArgs


class CreatePoolStep(StrictModel):
    id: str
    method: Literal["create_pool"]
    depends_on: list[str] = Field(default_factory=list)
    args: CreatePoolArgs


# class CreateJobScheduleStep(StrictModel):
#    id: str
#    method: Literal["create_job_schedule"]
#    depends_on: list[str] = Field(default_factory=list)
#    args: CreateJobScheduleArgs


class CreateJobStep(StrictModel):
    id: str
    method: Literal["create_job"]
    depends_on: list[str] = Field(default_factory=list)
    args: CreateJobArgs


class CreateBlobContainerStep(StrictModel):
    id: str
    method: Literal["create_blob_container"]
    depends_on: list[str] = Field(default_factory=list)
    args: CreateBlobContainerArgs


class DeleteJobStep(StrictModel):
    id: str
    method: Literal["delete_job"]
    depends_on: list[str] = Field(default_factory=list)
    args: DeleteJobArgs


class DeletePoolStep(StrictModel):
    id: str
    method: Literal["delete_pool"]
    depends_on: list[str] = Field(default_factory=list)
    args: DeletePoolArgs


class MonitorJobStep(StrictModel):
    id: str
    method: Literal["monitor_job"]
    depends_on: list[str] = Field(default_factory=list)
    args: MonitorJobArgs


class UploadFilesStep(StrictModel):
    id: str
    method: Literal["upload_files"]
    depends_on: list[str] = Field(default_factory=list)
    args: UploadFilesArgs


WorkflowStep = Union[
    AddTaskStep,
    CheckJobStatusStep,
    CreateBlobContainerStep,
    CreatePoolStep,
    CreateJobStep,
    DeleteJobStep,
    DeletePoolStep,
    MonitorJobStep,
    # CreateJobScheduleStep,
    UploadFilesStep,
]


class WorkflowPlan(BaseModel):
    goal: str
    workflow: list[WorkflowStep]


llm = ChatOpenAI(model="gpt-4.1", temperature=0)

structured_llm = llm.with_structured_output(WorkflowPlan)

with open("config/prompt_v4.txt", "r") as f:
    prompt_template = f.read()

prompt = ChatPromptTemplate.from_messages(
    [("system", prompt_template), ("human", "{goal}")]
)


def generate_workflow(goal: str) -> WorkflowPlan:
    chain = prompt | structured_llm
    return chain.invoke({"goal": goal})
