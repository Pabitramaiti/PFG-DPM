import boto3
import json
import os
from dpm_splunk_logger_py import splunk

ecs = boto3.client("ecs")

def lambda_handler(event, context):
    """
    Stop ECS tasks defined in the payload.
    Expected input structure:
      {
        "StatePayload": {
          "fileName": "abc_failed.json",
          "demo_task_list": [
            {"cluster": "cluster‑arn", "task_definition": "xyz:3", "container_name": "foo"},
            {"cluster": "cluster‑arn", "task_definition": "abc:5", "container_name": "bar"}
          ]
        }
      }
    """
    print("Event:", json.dumps(event))
    splunk.log_message({'Message': f"Received event: {json.dumps(event)}", "Status": "INFO"},context)
    state = event.get("StatePayload", {})

    file_name = state.get("fileName", "")
    task_list = state.get("TASK_LIST", [])

    # Decide outcome from file name
    if file_name.endswith("failed"):
        outcome = "failure"
    elif file_name.endswith("success"):
        outcome = "success"
    else:
        outcome = "unknown"

    reason = f"Triggered stop due to {outcome} file {file_name}"
    splunk.log_message({'Status': 'INFO', 'Message':  reason}, context)
    print(f"Outcome detected: {outcome}")

    stopped = 0
    details = []

    for task in task_list:
        cluster = task.get("cluster")
        task_definition = task.get("task_definition")

        if not cluster or not task_definition:
            print(f"Skipping item missing cluster or task_definition: {task}")
            splunk.log_message({'Status': "WARNING", 'Message': f'Skipping item missing cluster or task_definition: {task}'}, context)
            continue

        # Derive ECS family (everything before the colon)
        family = task_definition.split(":")[0]

        try:
            # List running tasks in that cluster/family
            running_arns = ecs.list_tasks(
                cluster=cluster,
                desiredStatus="RUNNING",
                family=family
            )["taskArns"]

            if not running_arns:
                print(f"No running tasks found for {family}")
              #  splunk.log_message("Status: 'INFO', Message: " + f"No running tasks found for {family}", context)
                splunk.log_message({'Status': 'INFO', 'Message': f"No running tasks found for {family}"}, context)
                continue

            for arn in running_arns:
                ecs.stop_task(cluster=cluster, task=arn, reason=reason[:255])
                print(f"Stop requested for {arn}")
                splunk.log_message({'Message': f"Stopped task {arn} in cluster {cluster} for reason: {reason}", "Status": "INFO"}, context)
                stopped += 1
                details.append({"taskArn": arn, "cluster": cluster})

        except Exception as e:
            print(f"Error while checking/stopping {family}: {e}")
            splunk.log_message({'Message': f"Error while checking/stopping {family}: {e}", "Status": "ERROR"}, context)
            raise e

    response = {
        "outcome": outcome,
        "stoppedCount": stopped,
        "stoppedTasks": details,
        "reason": reason
    }

    print("Response:", json.dumps(response))
    splunk.log_message({'Message': f"Lambda completed with response: {json.dumps(response)}", "Status": "INFO"}, context)
    return response