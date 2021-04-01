from typing import Any
from datetime import datetime
from prefect.tasks.shell import ShellTask
from prefect.utilities.tasks import defaults_from_attrs


class MetaflowShellTask(ShellTask):
    """
    Molded from https://github.com/PrefectHQ/prefect/blob/05cac2372c57a93ea72b05e7c844b1e115c01047/src/prefect/tasks/dbt/dbt.py#L9

    Task for running Metaflow flows. As there is currently no API to run flows within Python
    (see https://github.com/Netflix/metaflow/issues/116), we subclass the ShellTask and basically
    run as a command line something like:

    METAFLOW_PROFILE=my_profile python summation_flow.py run --sum=40

    It will create a profiles.yml file prior to running dbt commands.
    This task inherits all configuration options from the
    [ShellTask](https://docs.prefect.io/api/latest/tasks/shell.html#shelltask).
    Args:
        - command (string, optional): dbt command to be executed; can also be
            provided post-initialization by calling this task instance
        - env (dict, optional): dictionary of environment variables to use for
            the subprocess; can also be provided at runtime
        - helper_script (str, optional): a string representing a shell script, which
            will be executed prior to the `command` in the same process. Can be used to
            change directories, define helper functions, etc. when re-using this Task
            for different commands in a Flow; can also be provided at runtime
        - shell (string, optional): shell to run the command with; defaults to "bash"
        - return_all (bool, optional): boolean specifying whether this task should return all
            lines of stdout as a list, or just the last line as a string; defaults to `False`
        - log_stderr (bool, optional): boolean specifying whether this task
            should log the output from stderr in the case of a non-zero exit code;
            defaults to `False`
        - **kwargs: additional keyword arguments to pass to the Task constructor
    Example:
    """

    def __init__(
        self,
        flow_path: str = None,
        repo_name: str = None,  # needs to be owner/repo: jacopotagliabue/session-path
        clone_folder: str = None, # local folder to clone, needs to be != None if repo name is specified
        command: str = 'run',
        env: dict = None,
        helper_script: str = None,
        shell: str = "bash",
        return_all: bool = True,
        log_stderr: bool = True,
        **kwargs: Any
    ):
        self.command = command
        self.flow_path = flow_path
        self.repo_name = repo_name
        self.clone_folder = clone_folder
        # repo_name if and only if clone_folder
        assert (repo_name and clone_folder) or (not repo_name and not clone_folder)
        super().__init__(
            **kwargs,
            command=command,
            env=env,
            helper_script=helper_script,
            shell=shell,
            return_all=return_all,
            log_stderr=log_stderr
        )

    @defaults_from_attrs("command", "env", "helper_script")
    def run(
        self,
        command: str = None,
        flow_params: dict = None,
        env: dict = None,
        helper_script: str = None,
        token: str = None
    ) -> str:
        """
        If no profiles.yml file is found or if overwrite_profiles flag is set to True, this
        will first generate a profiles.yml file in the profiles_dir directory. Then run the dbt
        cli shell command.
        Args:
            - command (string): shell command to be executed; can also be
                provided at task initialization. Any variables / functions defined in
                `self.helper_script` will be available in the same process this command
                runs in
            - env (dict, optional): dictionary of environment variables to use for
                the subprocess
            - helper_script (str, optional): a string representing a shell script, which
                will be executed prior to the `command` in the same process. Can be used to
                change directories, define helper functions, etc. when re-using this Task
                for different commands in a Flow
        Returns:
            - stdout (string): if `return_all` is `False` (the default), only the last line of
                stdout is returned, otherwise all lines are returned, which is useful for
                passing result of shell command to other downstream tasks. If there is no
                output, `None` is returned.
        Raises:
            - prefect.engine.signals.FAIL: if command has an exit code other
                than 0
        """
        # clone repo into local folder if repo is specified
        if self.repo_name and self.clone_folder:
            # following https://github.com/PrefectHQ/prefect/blob/05cac2372c57a93ea72b05e7c844b1e115c01047/src/prefect/tasks/github/prs.py#L8
            # we import requests and interact with github only here at run
            import os
            import shutil
            import requests
            import zipfile
            from io import BytesIO
            # make sure folder is there and empty - delete first if it's there, and create empty
            if os.path.exists(self.clone_folder):
                shutil.rmtree(self.clone_folder)
            os.makedirs(self.clone_folder)
            # prepare and send request to github
            url = "https://api.github.com/repos/{}/zipball".format(self.repo_name)
            headers = {
                "AUTHORIZATION": "token {}".format(token),
                "Accept": "application/vnd.github.v3+json",
            }
            request = requests.get(url, headers=headers)
            zip_name = request.headers.get("Content-Disposition").split("filename=")[1]
            self.logger.info("Downloading file {} at {}".format(zip_name, datetime.utcnow()))
            # read the zip file in memory and extract it
            file = zipfile.ZipFile(BytesIO(request.content))
            file.extractall(self.clone_folder)
            # the folder name is the name of the zip, without the extension
            folder_name = zip_name.replace('.zip', '')
            # overwrite flow_path by pre-pending the cloned folder dir
            self.flow_path = os.path.join(self.clone_folder, folder_name, self.flow_path)
            self.logger.info("New local flow path is: {}".format(self.flow_path))

        # check if there are params
        if flow_params:
            command += ' '.join([' --{}={}'.format(k, v) for k, v in flow_params.items()])
        # build the final command
        metaflow_command = 'python {} {}'.format(self.flow_path, command)
        # log it for debugging purposes
        self.logger.info("Starting Metaflow run at {}, with cmd: {}".format(datetime.utcnow(), metaflow_command))
        # run it
        return super(MetaflowShellTask, self).run(
            command=metaflow_command, env=env, helper_script=helper_script
        )