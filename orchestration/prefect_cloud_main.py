import os
from dotenv import load_dotenv
# load envs
load_dotenv(verbose=True, dotenv_path='prefect.env')
# import prefect stuff
import prefect
from prefect import task, Flow, Parameter
from datetime import timedelta, datetime
from prefect.schedules import IntervalSchedule
from orchestration.MetaflowShellTask import MetaflowShellTask
# just make sure we have the required env variables for Prefect cloud to work...
assert os.getenv('PREFECT_AGENT_KEY') is not None


# set some parameters
x = Parameter('x', default=3, required=False)
y = Parameter('y', default=6, required=False)


# first prefect task sums two numbers - the sum is fed to Metaflow for the "summation flow"
@task
def prefect_sum(a: int, b: int):
    logger = prefect.context.get("logger")
    logger.info("Starting summation of {}, {} at {}".format(a, b, datetime.utcnow()))

    return a + b


@task(log_stdout=True)
def print_output(output):
    """
    Combination of this Github issue: https://github.com/PrefectHQ/prefect/issues/1013
    and log_stdout from https://docs.prefect.io/core/idioms/logging.html

    TODO: isn't there a better way to log from within the metaflow class?

    :param output: output of a Flow shell run
    :return:
    """
    print(output)


# instantiate Flow class for metaflow, adding a custom profile as an env, matching the desired aws setup.
# We assume a metaflow config is present (https://admin-docs.metaflow.org/overview/configuring-metaflow)
# but - as per metaflow standard - all configs can be overriden by the corresponding env variables, i.e.
# if you wish you can pass METAFLOW_JOB_QUEUE_NAME=my-queue as key value in the env params, and the flow
# will take that as the job queue name.
mf_flow = MetaflowShellTask(
    command='run', # run is the default for this type of task, but we specify it anyway for readability
    flow_path=os.getenv('METAFLOW_LOCAL_PATH'),
    env={'METAFLOW_PROFILE': 'metaflow'}
)

# instantiate schedule and compose tasks into a full DAG
schedule = IntervalSchedule(interval=timedelta(minutes=60))
with Flow(os.getenv('PREFECT_FLOW_NAME'), schedule) as flow:
    cnt_sum = prefect_sum(x, y)
    cnt_metaflow_average = mf_flow(flow_params={'sum': cnt_sum})
    print_output(cnt_metaflow_average)

# visualize it
flow.visualize(filename='flow_viz')
# remember to create the cloud project before running this!
flow.register(project_name=os.getenv('PREFECT_PROJECT_NAME'))
# run the agent
flow.run_agent(token=os.getenv('PREFECT_AGENT_KEY'), show_flow_logs=True)
