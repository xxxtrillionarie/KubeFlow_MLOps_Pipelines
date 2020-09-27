# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import json
import hashlib
import os
import sys
import re
import kubernetes
import yaml
import random
import argparse

from time import sleep
from metadata_helpers import *

parser = argparse.ArgumentParser(description='mlmd')
parser.add_argument('--inputs_json', type=str)
parser.add_argument('--outputs_json', type=str)
parser.add_argument('--argo_workflow_name', type=str)
parser.add_argument('--pod_name', type=str)
args = parser.parse_args()

#Connecting to MetadataDB
mlmd_store = connect_to_mlmd()

# create context if needed.
run_context = get_or_create_run_context(
    store=mlmd_store,
    run_id=args.argo_workflow_name, # We can switch to internal run IDs once backend starts adding them
)

# register execution
execution = create_new_execution_in_existing_run_context(
    store=mlmd_store,
    context_id=run_context.id,
    execution_type_name="testing",
    pod_name=args.pod_name,
    pipeline_name=args.argo_workflow_name,
    run_id=args.argo_workflow_name,
)

# get input artifacts
inputs = json.loads(args.inputs_json)
for artifact in inputs["inputs"]["artifacts"]:
    # Here we take a shortcut assuming the uri is constructed in a specific way.
    # Instead this should get the artifact by the execution and then its output artifacts.
    producer_task = artifact["value"]["producer_task"]
    output_artifact_key = artifact["value"]["output_artifact_key"]
    input_artifact = mlmd_store.get_artifacts_by_uri(uri='/'+'importer001'+'/'+output_artifact_key)[0]

# do execution
outputs = json.loads(args.outputs_json)
outputs["outputs"]["artifacts"][0]["value"]["custom_properties"][0]["value"] = input_artifact.custom_properties["accuracy"].int_value*2

# register output artifacts
for artifact in outputs["outputs"]["artifacts"]:
    custom_properties={}
    for cp in artifact["value"]["custom_properties"]:
        custom_properties[cp["key"]]=metadata_store_pb2.Value(int_value=cp["value"])
    create_new_artifact_event_and_attribution(
        store=mlmd_store,
        execution_id=execution.id,
        context_id=run_context.id,
        uri="/"+args.argo_workflow_name+'/'+artifact["key"],
        type_name='NoType',
        event_type=metadata_store_pb2.Event.OUTPUT,
        custom_properties=custom_properties,
    )

with open("/tmp/artifacts_step-one-output_custom_properties_accuracy","w+") as writer:
    writer.write(str(outputs["outputs"]["artifacts"][0]["value"]["custom_properties"][0]["value"]))