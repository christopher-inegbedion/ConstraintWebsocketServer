import asyncio
from constraint_models.create_constraint_util import CreateConstraintUtil
import threading
from constraints.constraint_main.constraint import Constraint
from constraints.enums.input_type import InputType
from constraints.enums.stage_status import StageStatus
import requests
import jsonpickle
from stage.stage import Stage, StageGroup
from task_main.task import Task
from task_pipeline.pipeline import Pipeline
from constraints.constraint_main.custom_constraint import CustomConstraint
import websockets
import nest_asyncio
from rich.traceback import install
import traceback
from collections import defaultdict

install()
nest_asyncio.apply()

all_tasks = {}
all_pipelines = {}


def tree(): return defaultdict(tree)


active_constraint_users = tree()

all_pipeline_details = {}
all_pipeline_owner_websockets = {}
admin_active_constraints_websockets = tree()
on_config_change_websockets = []

task_session_count_mutex = threading.Lock()
task_session_pending_users_mutex = threading.Lock()
task_session_active_users_mutex = threading.Lock()
task_session_complete_users_mutex = threading.Lock()

RESTserverBaseUrl = "http://localhost:8000/"

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

print("Started Pipeline server")


def event_handler(pipe, args):
    global loop
    loop.create_task(event_handler_implementation(args[0], pipe))


async def event_handler_implementation(websocket, pipe: Pipeline):
    recent_event = pipe.current_stage.log.most_recent_update
    data_to_send = {
        "event": recent_event["event"].name,
        "value": recent_event["value"],
        "msg": recent_event["msg"]
    }
    await websocket.send(jsonpickle.encode(data_to_send))


def on_config_handler(data, args):
    loop.create_task(on_config_handler_implementation(args[0], data))


async def on_config_handler_implementation(websocket, data):
    data_to_send = {"data": data}
    for socket in on_config_change_websockets:
        await socket.send(jsonpickle.encode(data_to_send))


def stage_complete_handler(pipe: Pipeline, args):
    global loop
    task_id = args[0]
    user_id = args[1]
    event = pipe.current_stage.log.most_recent_update

    if event["event"] == StageStatus.COMPLETE:
        print(event)
        if event["value"] == "Pending":
            loop.create_task(update_session_count(task_id))
            loop.create_task(
                update_pending_users_count(task_id, user_id))
        elif event["value"] == "Active":
            loop.create_task(
                update_active_users_count(task_id, user_id))
        elif event["value"] == "Complete":
            loop.create_task(
                update_complete_users_count(task_id, user_id))


def external_action_func(constraint_name, command, data, args):
    loop.create_task(external_action_func_implementation(
        args[0], constraint_name, command, data))


async def external_action_func_implementation(websocket, constraint_name, command, data):
    data_to_send = {
        "constraint_name": constraint_name,
        "command": command,
        "data": data
    }

    await websocket.send(jsonpickle.encode(data_to_send))


async def update_session_count(task_id):
    global all_pipeline_details
    task_session_count_mutex.acquire()
    session_count = all_pipeline_details[task_id]["session_count"]
    new_sess_count = session_count+1
    all_pipeline_details[task_id]["session_count"] = new_sess_count
    print("session count increased...")
    task_session_count_mutex.release()

    print("session counnt increment message sent...")
    if task_id in all_pipeline_owner_websockets:
        await all_pipeline_owner_websockets[task_id].send(jsonpickle.encode({"event": "count", "data": new_sess_count}))


async def update_pending_users_count(task_id, user_id):
    global all_pipeline_details
    task_session_pending_users_mutex.acquire()
    if user_id not in all_pipeline_details[task_id]["pending_users"]:
        all_pipeline_details[task_id]["pending_users"].append(user_id)
        print(f"User with ID: {user_id} started Pending stage")
    task_session_pending_users_mutex.release()

    if task_id in all_pipeline_owner_websockets:
        await all_pipeline_owner_websockets[task_id].send(jsonpickle.encode({"event": "new_pending_user", "data": user_id}))


async def update_active_users_count(task_id, user_id):
    global all_pipeline_details
    task_session_active_users_mutex.acquire()
    if user_id not in all_pipeline_details[task_id]["active_users"]:
        all_pipeline_details[task_id]["active_users"].append(user_id)
        print(f"User with ID: {user_id} started Active stage")
    task_session_active_users_mutex.release()

    if task_id in all_pipeline_owner_websockets:
        await all_pipeline_owner_websockets[task_id].send(jsonpickle.encode({"event": "new_active_user", "data": user_id}))


async def update_complete_users_count(task_id, user_id):
    global all_pipeline_details
    task_session_complete_users_mutex.acquire()
    if user_id not in all_pipeline_details[task_id]["complete_users"]:
        all_pipeline_details[task_id]["complete_users"].append(user_id)
        print(f"User with ID: {user_id} started Complete stage")
    task_session_complete_users_mutex.release()

    if task_id in all_pipeline_owner_websockets:
        await all_pipeline_owner_websockets[task_id].send(jsonpickle.encode({"event": "new_complete_user", "data": user_id}))


def get_constraint_config_inputs(constraint_name, stage_name, stage_group_id, task_id):
    stage_data = perform_network_action(
        RESTserverBaseUrl+"task/"+task_id+"/stage_group/"+stage_group_id + "/"+stage_name, "get")
    all_constraints = stage_data["constraints"]
    for constraint in all_constraints:
        if constraint["constraint_name"] == constraint_name:
            if constraint["config_inputs"] == {}:
                return None

            return constraint["config_inputs"]

    return None


async def launch(websocket, path):
    global all_pipeline_details, all_pipelines, all_pipeline_owner_websockets

    # Start a pipeline.
    if path == "/start_pipeline":
        data = jsonpickle.decode(await websocket.recv())
        user_id = data["user_id"]
        stage_name = data["stage_name"]
        task_id = data["task_id"]
        print(f"Start pipeline command. User ID: {user_id}...")

        if user_id not in all_pipelines[task_id]["sessions"]:
            print(f"Creating new pipeline instance for user: {user_id}")
            # Get the task's details from the DB
            task_data = perform_network_action(
                RESTserverBaseUrl+"task/"+task_id, "get")
            task_name = task_data["name"]
            task_desc = task_data["desc"]

            if "task_properties" in task_data:
                task_properties = task_data["task_properties"]
            else:
                task_properties = {}

            stage_group_id = task_data["stage_group_id"]
            price = task_data["price"]
            currency = task_data["currency"]
            price_constraint = task_data["price_constraint_name"]
            price_constraint_stage = task_data["price_constraint_stage"]
            if task_data["msg"] == "success":
                print("Task details retrieved from database...")
                try:
                    new_task = Task(task_name, task_desc)

                    # set the properties for the task
                    for property in task_properties:
                        i = task_properties[property]
                        new_task.add_property(
                            i["name"], i["value"], i["denomination"])

                    # load stage group data from the REST server
                    stage_group_data = perform_network_action(
                        RESTserverBaseUrl+"stage_group/"+stage_group_id, "get")
                    print("Stage group details retrieved from database...")
                    new_stage_groups = StageGroup()
                    stages = stage_group_data["stages"]
                    for stage in stages:
                        new_stage = Stage(stage["stage_name"])
                        for constraint in stage["constraints"]:
                            constraint_details: Constraint = CreateConstraintUtil.create_constraint(
                                constraint)

                            new_constraint = CustomConstraint(
                                constraint_details.name, constraint_details.description, constraint_details.model)
                            config_inputs = get_constraint_config_inputs(
                                constraint, stage["stage_name"], stage_group_id, task_id)
                            if config_inputs != None:
                                for i in config_inputs:
                                    new_constraint.add_configuration_input(
                                        config_inputs[i], key=i)

                            new_stage.add_constraint(new_constraint)
                        new_stage_groups.add_stage(new_stage)

                    # set the stage group for the task
                    new_task.price = float(price)
                    new_task.currency = currency
                    new_task.set_constraint_stage_config(new_stage_groups)
                    new_task.set_price_constraint(
                        CreateConstraintUtil.create_constraint(price_constraint))
                    new_task.price_constraint_stage = price_constraint_stage
                    pipeline = Pipeline(new_task, new_stage_groups)
                    # ^ Task and Pipeline object have been created

                    # Save the pipeline object for the user and initialize it in all_pipeline_details
                    all_pipelines[task_id]["sessions"][user_id] = pipeline

                    print("pipeline created...")
                    pipeline.start()
                    pipeline.on_stage_complete(
                        stage_complete_handler,  task_id, user_id, stage_name="",)
                    print(f"Stage: {stage_name} started...")
                    print()

                    await websocket.send(jsonpickle.encode({
                        "result": "success"
                    }))
                except Exception as e:
                    traceback.print_exc()

                    print(e)
                    await websocket.send(jsonpickle.encode({
                        "result": "fail"
                    }))

    # Start constraint pt1. This is called when the constraint page is loaded.
    # If the constraint does not requires initial input it will be started, else
    # the input will be passed from the "/start_constraint2" endpoint.
    elif path == "/start_constraint1":
        data = jsonpickle.decode(await websocket.recv())
        user_id = data["user_id"]
        constraint_name = data["constraint_name"]
        stage_name = data["stage_name"]
        task_id = data["task_id"]
        # print(
        #     f"command to start constraint: [{constraint_name}] in stage: [{stage_name}] with task id: [{task_id}]")

        pipeline = all_pipelines[task_id]["sessions"][user_id]

        print("starting constraint...")
        constraint = pipeline.get_constraint(constraint_name, stage_name)

        # Check if the constraint requires input
        is_constraint_input_required = constraint.model.input_count != 0

        print("requesting input...")
        print()

        if is_constraint_input_required:
            print(f"constraint [{constraint_name}] requires input")

            # Request input
            await websocket.send(
                jsonpickle.encode({
                    "event": "INPUT_REQUIRED",
                    "value": {"input_count": constraint.model.input_count, "input_type": str(constraint.model.input_type)}
                })
            )
        else:
            print(f"constraint [{constraint_name}] does not require input")

            await websocket.send(
                jsonpickle.encode({
                    "event": "INPUT_NOT_REQUIRED"
                })
            )

            pipeline.start_constraint(stage_name, constraint_name)

        if "started_users" not in all_pipelines[task_id]:
            all_pipelines[task_id]["started_users"] = {}

        if stage_name not in all_pipelines[task_id]["started_users"]:
            all_pipelines[task_id]["started_users"][stage_name] = {}

        if constraint_name not in all_pipelines[task_id]["started_users"][stage_name]:
            all_pipelines[task_id]["started_users"][stage_name][constraint_name] = [
            ]

        all_pipelines[task_id]["started_users"][stage_name][constraint_name].append(
            user_id)

    # Start constraint with some data provided. This endpoint is for constraints that requires input
    elif path == "/start_constraint2":
        # receive constraint input data
        constraint_inputs = jsonpickle.decode(await websocket.recv())

        data = constraint_inputs["data"]
        user_id = constraint_inputs["user_id"]
        constraint_name = constraint_inputs["constraint_name"]
        stage_name = constraint_inputs["stage_name"]
        task_id = constraint_inputs["task_id"]
        print(
            f"data recieved for constraint: [{constraint_name}] in stage: [{stage_name}] with task id: [{task_id}]")

        if task_id == None:
            raise Exception(f"Task with ID: {task_id} cannot be found")

        if user_id == None:
            raise Exception(f"User with ID: {user_id} cannot be found")

        pipeline: Pipeline = all_pipelines[task_id]["sessions"][user_id]
        print(f"running for stage: {stage_name}")
        constraint_obj = pipeline.get_constraint(
            constraint_name, stage_name)

        if constraint_inputs["response"] == "INPUT_REQUIRED":
            for constraint_input in data:
                if constraint_obj.model.input_type == InputType.INT:
                    constraint_obj.add_input(int(constraint_input))
                elif constraint_obj.model.input_type == InputType.STRING:
                    constraint_obj.add_input(str(constraint_input))
                else:
                    constraint_obj.add_input(constraint_input)

        pipeline.start_constraint(stage_name, constraint_name)
        print()

    # Register a user as active when they are in the constraints view page. When a user
    # loads the constraint, they are registered as an active user and can be interacted with
    # by the admin if the constraint allows it. If the admin is active, also notify them
    # of a new user
    elif path == "/register_active_user":
        data = jsonpickle.decode(await websocket.recv())
        user_id = data["user_id"]
        constraint_name = data["constraint_name"]
        stage_name = data["stage_name"]
        task_id = data["task_id"]

        active_constraint_users[task_id][stage_name][constraint_name].setdefault(
            "active_users", [])
        active_users = active_constraint_users[task_id][stage_name][constraint_name].get(
            "active_users", [])

        if user_id not in active_constraint_users[task_id][stage_name][constraint_name]["active_users"]:
            active_constraint_users[task_id][stage_name][constraint_name]["active_users"].append(
                user_id)

        # notify the admin that a new user is active on a constraint
        admin_websocket = admin_active_constraints_websockets[
            task_id][stage_name].get(constraint_name, None)
        if admin_websocket != None:
            active_users: list = active_constraint_users[task_id][
                stage_name][constraint_name].get(
                "active_users", [])
            await admin_websocket.send(jsonpickle.encode({
                "active_users": active_users
            }))

        await websocket.send(jsonpickle.encode({
            "msg": "done"
        }))

    # Unregister a user as active, when they leave the constraint's view page or suspend the app.
    elif path == "/unregister_active_user":
        data = jsonpickle.decode(await websocket.recv())
        user_id = data["user_id"]
        constraint_name = data["constraint_name"]
        stage_name = data["stage_name"]
        task_id = data["task_id"]

        try:
            active_users: list = active_constraint_users[task_id][stage_name][constraint_name].get(
                "active_users", [])
            if user_id in active_users:
                active_users.remove(user_id)

            # notify the admin that a user is no longer active
            admin_websocket = admin_active_constraints_websockets[
                task_id][stage_name].get(constraint_name, None)
            if admin_websocket != None:
                active_users: list = active_constraint_users[task_id][
                    stage_name][constraint_name].get(
                    "active_users", [])
                await admin_websocket.send(jsonpickle.encode({
                    "active_users": active_users
                }))

            await websocket.send(jsonpickle.encode({
                "msg": "done"

            }))
        except KeyError:
            await websocket.send(jsonpickle.encode({
                "msg": "error"
            }))

    # Return all the active users for a task's constraint
    elif path == "/get_constraint_active_users":
        data = jsonpickle.decode(await websocket.recv())
        constraint_name = data["constraint_name"]
        task_id = data["task_id"]
        stage_name = data["stage_name"]

        admin_active_constraints_websockets[task_id][stage_name].setdefault(
            constraint_name, websocket)
        admin_active_constraints_websockets[task_id][stage_name][constraint_name] = websocket

        try:
            active_users: list = active_constraint_users[task_id][stage_name][constraint_name].get(
                "active_users", [])
            await admin_active_constraints_websockets[task_id][stage_name].get(constraint_name, websocket).send(jsonpickle.encode(
                {
                    "active_users": active_users
                }
            ))
        except KeyError:
            await admin_active_constraints_websockets[task_id][stage_name].get(constraint_name, websocket).send(jsonpickle.encode(
                {
                    "active_users": "error"
                }
            ))

    # Unregister the admin session from listening to active users . When removed this would prevent the admin
    # from being notified of a new active user when a constraint starts
    elif path == "/disconnect_from_constraint_active_users":
        data = jsonpickle.decode(await websocket.recv())
        constraint_name = data["constraint_name"]
        task_id = data["task_id"]
        stage_name = data["stage_name"]

        if admin_active_constraints_websockets[task_id][stage_name].get(constraint_name, None) != None:
            admin_active_constraints_websockets[task_id][stage_name][constraint_name] = None

    # Return a constraint's configuration inputs
    elif path == "/get_constraint_config_inputs":
        data = jsonpickle.decode(await websocket.recv())
        constraint_name = data["constraint_name"]
        stage_name = data["stage_name"]
        task_id = data["task_id"]
        user_id = data["user_id"]
        pipeline: Pipeline = all_pipelines[task_id]["sessions"][user_id]
        constraint: Constraint = pipeline.get_constraint(
            constraint_name, stage_name)

        await websocket.send(jsonpickle.encode(constraint.configuration_inputs))

    # Notify the websocket when a constraint completes
    elif path == "/on_constraint_complete":
        constraint_inputs = jsonpickle.decode(await websocket.recv())
        constraint_name = constraint_inputs["constraint_name"]
        stage_name = constraint_inputs["stage_name"]
        task_id = constraint_inputs["task_id"]
        user_id = constraint_inputs["user_id"]

        pipeline = all_pipelines[task_id]["sessions"][user_id]
        pipeline.on_constraint_complete(
            event_handler, websocket, constraint_name)
        print(f"client listening to {constraint_name} changes")
        print()

    # Notify the websocket of a constraint external action event
    elif path == "/listen_external_action":
        data = jsonpickle.decode(await websocket.recv())
        constraint_name = data["constraint_name"]
        stage_name = data["stage_name"]
        task_id = data["task_id"]
        user_id = data["user_id"]
        pipeline: Pipeline = all_pipelines[task_id]["sessions"][user_id]
        constraint: Constraint = pipeline.get_constraint(
            constraint_name, stage_name)
        constraint.on_external_action(external_action_func, websocket)

    # Return the status of a stage
    elif path == "/get_stage_status":
        constraint_inputs = jsonpickle.decode(await websocket.recv())
        stage_name = constraint_inputs["stage_name"]
        task_id = constraint_inputs["task_id"]
        user_id = constraint_inputs["user_id"]

        print(f"user: {user_id} checking if {stage_name} is running...")
        # Check if a session has been created for the task with id [task_id]
        if task_id not in all_pipelines:
            all_pipelines[task_id] = {"sessions": {}}
            all_pipeline_details[task_id] = {"session_count": 0, "pending_users": [
            ], "active_users": [], "complete_users": []}

        if user_id in all_pipelines[task_id]["sessions"]:
            pipeline = all_pipelines[task_id]["sessions"][user_id]
            stage = pipeline.get_stage(stage_name)
            print(f"{stage_name} -> {pipeline.get_stage(stage_name).status}")

            if pipeline.get_stage(stage_name).status == StageStatus.COMPLETE:
                await websocket.send(jsonpickle.encode({"value": "complete"}))
            elif pipeline.get_stage(stage_name).status == StageStatus.ACTIVE:
                await websocket.send(jsonpickle.encode({"value": "running"}))
            elif pipeline.get_stage(stage_name).status == StageStatus.NOT_STARTED:
                await websocket.send(jsonpickle.encode({"value": "not_started"}))
            elif pipeline.get_stage(stage_name).status == StageStatus.CONSTRAINT_STARTED:
                await websocket.send(jsonpickle.encode({"value": "constraint_active", "constraint": pipeline.get_active_constraint()[0].name}))
            elif pipeline.get_stage(stage_name).status == StageStatus.CONSTRAINT_COMPLETED:
                await websocket.send(jsonpickle.encode({"value": "running"}))

        else:
            await websocket.send(jsonpickle.encode({"value": "not_started"}))
        print()

    # Return "running" if stage is running, else return "not_running"
    elif path == "/is_pipe_running":
        data = jsonpickle.decode(await websocket.recv())
        task_id = data["task_id"]
        user_id = data["user_id"]

        if user_id in all_pipelines[task_id]["sessions"]:
            pipeline = all_pipelines[task_id]["sessions"][user_id]
            if pipeline.current_stage != None:
                await websocket.send(jsonpickle.encode({"value": "running"}))
            else:
                await websocket.send(jsonpickle.encode({"value": "not_running"}))
        else:
            await websocket.send(jsonpickle.encode({"value": "not_running"}))

    elif path == "/connect_task_details":
        task_id = jsonpickle.decode(await websocket.recv())["task_id"]
        all_pipeline_owner_websockets[task_id] = websocket
        print("client listening to task session added...")

    elif path == "/disconnect_task_details":
        task_id = jsonpickle.decode(await websocket.recv())["task_id"]

        if task_id in all_pipeline_owner_websockets:
            all_pipeline_owner_websockets.pop(task_id)
            print("client listening to session removed")

    elif path == "/pipeline_details":
        data = jsonpickle.decode(await websocket.recv())
        user_id = data["user_id"]
        task_id = data["task_id"]

        pipeline: Pipeline = all_pipelines[task_id]["sessions"][user_id]
        data = pipeline.get_stage_group_details()
        await websocket.send(jsonpickle.encode({"value": data}))

    elif path == "/pipeline_session_admin_details":
        data = jsonpickle.decode(await websocket.recv())
        task_id = data["task_id"]

        print(all_pipeline_details)
        if task_id in all_pipeline_details:
            pipeline_details = all_pipeline_details[task_id]
            data_to_send = {"session_count": pipeline_details["session_count"], "pending_users": pipeline_details["pending_users"],
                            "active_users": pipeline_details["active_users"], "complete_users": pipeline_details["complete_users"]}
            await websocket.send(jsonpickle.encode(data_to_send))

    # Return the next constraint or stage from a constraint
    elif path == "/get_next_constraint_or_stage":
        data = jsonpickle.decode(await websocket.recv())
        current_constraint = data["constraint_name"]
        current_stage = data["stage_name"]
        user_id = data["user_id"]
        task_id = data["task_id"]

        pipeline: Pipeline = all_pipelines[task_id]["sessions"][user_id]

        await websocket.send(jsonpickle.encode(pipeline.get_next_constraint_or_stage(current_stage, current_constraint)))

    # Notify the websocket when a change in the constraint's configuration input values change
    elif path == "/on_config_change":
        data = jsonpickle.decode(await websocket.recv())
        user_id = data["user_id"]
        constraint_name = data["constraint_name"]
        stage_name = data["stage_name"]
        task_id = data["task_id"]

        pipeline: Pipeline = all_pipelines[task_id]["sessions"][user_id]
        constraint: Constraint = pipeline.get_constraint(
            constraint_name, stage_name)
        on_config_change_websockets.append(websocket)
        constraint.on_config_action(on_config_handler, websocket)

    # Send a listen command to the constraint
    elif path == "/send_listen_data":
        data = jsonpickle.decode(await websocket.recv())
        user_id = data["user_id"]
        constraint_name = data["constraint_name"]
        stage_name = data["stage_name"]
        task_id = data["task_id"]
        command_msg = data["command_msg"]
        command_data = data["command_data"]

        pipeline: Pipeline = all_pipelines[task_id]["sessions"][user_id]
        constraint: Constraint = pipeline.get_constraint(
            constraint_name, stage_name)
        constraint.send_listen_data(command_msg, command_data)

    while websocket.open:
        await asyncio.sleep(0)


def perform_network_action(addr, method, data=None):
    if method == "get":
        r = requests.get(addr)

    elif method == "post":
        r = requests.post(addr, data=data)

    if r.status_code == 200:
        result = r.json()
        return result
    else:
        return None


start_server = websockets.serve(launch, "0.0.0.0", 4321)

loop.run_until_complete(start_server)
loop.run_forever()
