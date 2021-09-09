import os 
import reana_client as rc


def read_spec_file(spec_file_name):
  # parameters: file name for REANA workflow specfication

  # Reads the spec file and converts into a Python dict
  contents = None
  with open(spec_file_name, 'r') as stream:
        contents = yaml.safe_load(stream)
  return contents


def get_worfklow_from_spec_file(workflow_file):
  # parameters: file name for REANA workflow specfication
  # reads the spec file and returns the workflow dictionary
  spec = read_spec_file(workflow_file)
  if('workflow' not in spec.keys()):
    raise Exception('\"workflow\" field is not in the yaml file')
  workflow_dict = spec['workflow']

  req_keys = ['inputs', 'workflow', 'outputs']
  if not all(key in  workflow_spec.keys() for key in req_keys): 
    raise Exception('workflow spec does not have \"input\", \"workflow\" or \"output\"')

  return workflow_dict


def create_workflow(workflow_name, worflow_dict, reana_token): 
   # parameters: Workflow dictionary
   #             Workflow name and the REANA token 

   # Creates a workflow for the given workflow dictionary
  if 'inputs' not in worflow_dict:
    raise Exception('inputs filed not specified in workflow')
  
  inputs = workflow_dict['inputs']
  from reana_client.api.client import create_workflow_from_json
  
  wf_json = workflow_dict['workflow']['specification']

  response_json = rc.create_workflow_from_json(
                        workflow_json=wf_json,
                        name=workflow_name,
                        access_token=reana_token,
                        parameters=inputs,
                        workflow_engine='serial')
  
  return response_json

def uplaod_input_files(workflow_name, workflow_file, reana_token): 
  # Uploads the input files to the REANA server
  dir_path = os.path.dirname(os.path.realpath('reana.yaml'))
  os.chdir(dir_path)
  from reana_client.api.client import upload_to_server
  wf_name = 'rob_reana_colab'
  abs_path_to_input_files = [os.path.abspath(f) for f in inputs['files']]
  rc.upload_to_server(workflow_name, abs_path_to_input_files, my_reana_token)

def monitor_workflow(workflow_name,reana_token, time_interval=5):
  status = None
  while status != 'finished' or status != 'failed':
    status_details = rc.get_workflow_status(workflow_name, reana_token)
    status = status_details['status']
    print('Current status: ', status)
    time.sleep(time_interval)

def start_workflow(workflow_name, reana_token): 
   rc.start_workflow(workflow_name, reana_token, {})

def reana_exec(workflow_name, workflow_file, reana_token): 
  workflow_dict = get_worfklow_from_spec_file(workflow_file)
  create_workflow(workflow_name, worflow_dict, reana_token)
  uplaod_input_files(workflow_name, workflow_file, reana_token)
  start_workflow(workflow_name, reana_token)
  monitor_workflow(workflow_name,reana_token)


  
