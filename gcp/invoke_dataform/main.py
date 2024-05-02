import asyncio
import os

from google.cloud import datastore
from google.cloud.datastore.query import PropertyFilter

from google.cloud import dataform_v1beta1

from fastapi import FastAPI
from fastapi.responses import JSONResponse

PROJECT = os.environ.get("GOOGLE_APPLICATION_PROJECT")
REGION = os.environ.get("GCLOUD_APPLICATION_LOCATION", "us-central1")
REPOSITORY = os.environ.get("DATAFORM_REPOSITORY")
WORKSPACE = os.environ.get("WORKSPACE", "production")
GITHUB_AUTHOR_NAME = os.environ.get("GITHUB_AUTHOR_NAME")
GITHUB_AUTHOR_EMAIL = os.environ.get("GITHUB_AUTHOR_EMAIL")


FAILURE_WORKFLOW_STATE = [
    dataform_v1beta1.WorkflowInvocation.State.CANCELING,
    dataform_v1beta1.WorkflowInvocation.State.CANCELLED,
    dataform_v1beta1.WorkflowInvocation.State.FAILED,
]

TERMINAL_WORKFLOW_ACTION_STATE = [
    dataform_v1beta1.WorkflowInvocationAction.State.CANCELLED,
    dataform_v1beta1.WorkflowInvocationAction.State.DISABLED,
    dataform_v1beta1.WorkflowInvocationAction.State.FAILED,
    dataform_v1beta1.WorkflowInvocationAction.State.SKIPPED,
    dataform_v1beta1.WorkflowInvocationAction.State.SUCCEEDED,
]

async def pull_git_commit(
    dataform_client: dataform_v1beta1.DataformAsyncClient,
    workspace: str) -> None:
    
    author = dataform_v1beta1.CommitAuthor()
    author.name = GITHUB_AUTHOR_NAME
    author.email_address = GITHUB_AUTHOR_EMAIL
    
    request_pull_git_commit = dataform_v1beta1.PullGitCommitsRequest()
    request_pull_git_commit.name = workspace
    request_pull_git_commit.author = author
    
    return await dataform_client.pull_git_commits(
        request=request_pull_git_commit
    )

async def create_compilation_result(
    dataform_client: dataform_v1beta1.DataformAsyncClient,
    parent: str,
    workspace: str) -> dataform_v1beta1.CompilationResult:
    
    # Creamos el CompilationResult
    compilation_result = dataform_v1beta1.CompilationResult()
    compilation_result.workspace = workspace
    
    # Creamos la request del CompilationResult
    request_compilation_result = dataform_v1beta1.CreateCompilationResultRequest(
        parent=parent,
        compilation_result=compilation_result
    )
    
    return await dataform_client.create_compilation_result(request=request_compilation_result)


async def retrieve_dataform_metadata_from_datastore(project: str, source: str) -> dict:
    datastore_client = datastore.Client()
    query = datastore_client.query(kind="dataform_target")
    query.add_filter(filter=PropertyFilter("source", "=", source))
    
    targets = []
    tags = set()

    include_dependencies = False
    include_dependents = False
    full_refresh = False
    
    for entity in query.fetch():
        target = dataform_v1beta1.Target()
        target.name = entity["dependent"]
        target.schema = entity["dataset"]
        target.database = project
        
        tags = tags.union(entity["tags"])
        
        if entity["include_dependencies"]:
            include_dependencies = True
        
        if entity["include_dependents"]:
            include_dependents = True
            
        if entity["full_refresh"]:
            full_refresh = True
            
        targets.append(target)
    
    return dict(
        targets=targets,
        tags=tags,
        include_dependencies=include_dependencies,
        include_dependents=include_dependents,
        full_refresh=full_refresh
    )

async def create_invocation_config(
    project: str,
    source: str) -> dataform_v1beta1.InvocationConfig:
    
    # datastore_output = await retrieve_dataform_metadata_from_datastore(project, source)
    
    # invocation_config = dataform_v1beta1.InvocationConfig()
    # invocation_config.included_targets = datastore_output["targets"]
    # invocation_config.included_tags = datastore_output["tags"]
    # invocation_config.transitive_dependencies_included = datastore_output["include_dependencies"]
    # invocation_config.transitive_dependents_included = datastore_output["include_dependents"]
    
    target = dataform_v1beta1.Target()
    target.database = project
    target.schema = "raw"
    target.name = source
    
    invocation_config = dataform_v1beta1.InvocationConfig()
    invocation_config.included_targets = [target]

    invocation_config.transitive_dependencies_included = True
    invocation_config.transitive_dependents_included = True
    
    return invocation_config
    
async def create_workflow_invocation(
    dataform_client: dataform_v1beta1.DataformAsyncClient,
    parent: str,
    invocation_config: dataform_v1beta1.InvocationConfig,
    compilation_result: dataform_v1beta1.CompilationResult) -> dataform_v1beta1.WorkflowInvocation:
    
    # Creamos el WorkflowInvocation object
    workflow_invocation = dataform_v1beta1.WorkflowInvocation()
    workflow_invocation.compilation_result = compilation_result.name
    workflow_invocation.invocation_config = invocation_config
    
    # Creamos la request del WorkflowInvocation
    request_workflow_invocation = dataform_v1beta1.CreateWorkflowInvocationRequest(
        parent=parent,
        workflow_invocation=workflow_invocation,
    )
    
    # Enviamos la request
    return await dataform_client.create_workflow_invocation(request=request_workflow_invocation)
    
async def wait_until_finish_workflow_invocation(
    dataform_client: dataform_v1beta1.DataformAsyncClient,
    workflow_invocation: dataform_v1beta1.WorkflowInvocation,
    expected_state: str = dataform_v1beta1.WorkflowInvocation.State.SUCCEEDED,
    pool_sleep: int = 10) -> bool:
    
    actions = {}
    
    while True:
        request_get_workflow_invocation = dataform_v1beta1.GetWorkflowInvocationRequest()
        request_get_workflow_invocation.name = workflow_invocation.name
        workflow_invocation = await dataform_client.get_workflow_invocation(request=request_get_workflow_invocation)
        
        request_workflow_invocation_actions = dataform_v1beta1.QueryWorkflowInvocationActionsRequest(name=workflow_invocation.name)
        page_result_workflow_invocation_actions = await dataform_client.query_workflow_invocation_actions(request=request_workflow_invocation_actions)
        
        async for workflow_invocation_actions in page_result_workflow_invocation_actions:
            target = workflow_invocation_actions.target
            action_id = f"{workflow_invocation_actions.target.database}.{workflow_invocation_actions.target.schema}.{workflow_invocation_actions.target.name}"
            
            action_state = workflow_invocation_actions.state
            action_invocation_time =  workflow_invocation_actions.invocation_timing
            
            if action_state in TERMINAL_WORKFLOW_ACTION_STATE and action_id not in actions:
                actions[action_id] = action_state.name
                print(f"WorkflowAction {action_id} terminado con state: {action_state.name}")
                
                try:
                    action_duration = action_invocation_time.end_time["seconds"] - action_invocation_time.start_time["seconds"]
                    print(action_duration)
                except:
                    continue
                
            elif action_state == dataform_v1beta1.WorkflowInvocationAction.State.RUNNING:
                print(f"WorkflowAction {action_id} ejecutandose...")
            else:
                pass
        
        if workflow_invocation.state == expected_state:
            print(f"{workflow_invocation.name} terminado con state: {workflow_invocation.state.name}")
            return dict(
                workflow_invocation_name=workflow_invocation.name,
                workflow_invocation_state=workflow_invocation.state.name,
                workflow_actions=actions
            )
        elif workflow_invocation.state in FAILURE_WORKFLOW_STATE:
            error = f"{workflow_invocation.name} fallado con state: {workflow_invocation.state.name}"
            print(error)
            return dict(
                workflow_invocation_name=workflow_invocation.name,
                workflow_invocation_state=workflow_invocation.state.name,
                workflow_actions=actions
            )
        
        print("Workflow se sigue ejecutando...\n")
        await asyncio.sleep(pool_sleep)
    
app = FastAPI()

@app.get("/")
async def main(source: str| None=None):
    """Ejecucion de un workflow. Primero se compila el workspace y luego se ejecutan los actions especificados."""
    
    project = PROJECT
    region = REGION
    repo = REPOSITORY
    workspace_name = WORKSPACE
    
    if source is None:
        return JSONResponse(content="source required")
    
    parent = f"projects/{project}/locations/{region}/repositories/{repo}"
    workspace = f"{parent}/workspaces/{workspace_name}"
    
    # Cliente Dataform
    dataform_client = dataform_v1beta1.DataformAsyncClient()
    
    # Pull de branch main al workspace producion (nos bajamos la ultima version)
    print(f"Pull la ultima version del default branch en {workspace_name}")
    await pull_git_commit(
        dataform_client=dataform_client,
        workspace=workspace,   
    )
    
    print(f"Ejecutamos la compilacion del workspace {workspace_name}")
    response_compilation_result = await create_compilation_result(
        dataform_client=dataform_client,
        parent=parent,
        workspace=workspace)
    
    print("Creamos la configuracion del workflow invocation")
    invocation_config = await create_invocation_config(project=project, source=source)
    
    print("Invocamos la ejecucion del workflow")
    response_workflow_invocation = await create_workflow_invocation(
        dataform_client=dataform_client,
        parent=parent,
        invocation_config=invocation_config,
        compilation_result=response_compilation_result)
    
    print(f"Esperamos a que termine el workflow {response_workflow_invocation.name}\n")
    workflow_invocation_info = await wait_until_finish_workflow_invocation(
        dataform_client=dataform_client,
        workflow_invocation=response_workflow_invocation
    )
    return JSONResponse(content=workflow_invocation_info)