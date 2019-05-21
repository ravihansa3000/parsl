import logging

from parsl.app.futures import DataFuture
from parsl.data_provider.files import File
from parsl.data_provider.file_noop import NoOpFileStaging
from parsl.data_provider.ftp import FTPSeparateTaskStaging
from parsl.data_provider.http import HTTPSeparateTaskStaging

logger = logging.getLogger(__name__)

# these will be shared between all executors that do not explicitly
# override, so should not contain executor-specific state
defaultStaging = [NoOpFileStaging(), FTPSeparateTaskStaging(), HTTPSeparateTaskStaging()]


class DataManager(object):
    """The DataManager is responsible for transferring input and output data.

    """

    def __init__(self, dfk):
        """Initialize the DataManager.

        Args:
           - dfk (DataFlowKernel): The DataFlowKernel that this DataManager is managing data for.

        Kwargs:
           - executors (list of Executors): Executors for which data transfer will be managed.
        """

        self.dfk = dfk
        self.globus = None

    def replace_task_stage_out(self, file, func, executor):
        """This will give staging providers the chance to wrap (or replace entirely!) the task function."""
        executor_obj = self.dfk.executors[executor]
        if hasattr(executor_obj, "storage_access") and executor_obj.storage_access is not None:
            storage_access = executor_obj.storage_access
        else:
            storage_access = defaultStaging

        for scheme in storage_access:
            logger.debug("stage_out checking Staging provider {}".format(scheme))
            if scheme.can_stage_out(file):
                newfunc = scheme.replace_task_stage_out(self, executor, file, func)
                if newfunc:
                    return newfunc
                else:
                    return func

        logger.debug("reached end of staging scheme list")
        # if we reach here, we haven't found a suitable staging mechanism
        raise ValueError("Executor {} cannot stage file {}".format(executor, repr(file)))

    def replace_task(self, input, func, executor):
        """This will give staging providers the chance to wrap (or replace entirely!) the task function."""

        if isinstance(input, DataFuture):
            file = input.file_obj
        elif isinstance(input, File):
            file = input
        else:
            return func

        executor_obj = self.dfk.executors[executor]
        if hasattr(executor_obj, "storage_access") and executor_obj.storage_access is not None:
            storage_access = executor_obj.storage_access
        else:
            storage_access = defaultStaging

        for scheme in storage_access:
            logger.debug("stage_in checking Staging provider {}".format(scheme))
            if scheme.can_stage_in(file):
                newfunc = scheme.replace_task(self, executor, file, func)
                if newfunc:
                    return newfunc
                else:
                    return func

        logger.debug("reached end of staging scheme list")
        # if we reach here, we haven't found a suitable staging mechanism
        raise ValueError("Executor {} cannot stage file {}".format(executor, repr(file)))

    def stage_in(self, input, executor):
        """Transport the input from the input source to the executor, if it is file-like,
        returning a DataFuture that wraps the stage-in operation.

        If no staging in is required - because the `file` parameter is not file-like,
        then return that parameter unaltered.

        Args:
            - self
            - input (Any) : input to stage in. If this is a File or a
              DataFuture, stage in tasks will be launched with appropriate
              dependencies. Otherwise, no stage-in will be performed.
            - executor (str) : an executor the file is going to be staged in to.
        """

        if isinstance(input, DataFuture):
            file = input.file_obj
            parent_fut = input
        elif isinstance(input, File):
            file = input
            parent_fut = None
        else:
            return input

        executor_obj = self.dfk.executors[executor]
        if hasattr(executor_obj, "storage_access") and executor_obj.storage_access is not None:
            storage_access = executor_obj.storage_access
        else:
            storage_access = defaultStaging

        for scheme in storage_access:
            logger.debug("stage_in checking Staging provider {}".format(scheme))
            if scheme.can_stage_in(file):
                staging_fut = scheme.stage_in(self, executor, file, parent_fut=parent_fut)
                if staging_fut:
                    return staging_fut
                else:
                    return input

        logger.debug("reached end of staging scheme list")
        # if we reach here, we haven't found a suitable staging mechanism
        raise ValueError("Executor {} cannot stage file {}".format(executor, repr(file)))

    def stage_out(self, file, executor, app_fu):
        """Transport the file from the local filesystem to the remote Globus endpoint.

        This function returns ? (previously documentated as a DataFuture - but not used)

        Args:
            - self
            - file (File) - file to stage out
            - executor (str) - Which executor the file is going to be staged out from.
        """
        executor_obj = self.dfk.executors[executor]
        if hasattr(executor_obj, "storage_access") and executor_obj.storage_access is not None:
            storage_access = executor_obj.storage_access
        else:
            storage_access = defaultStaging

        for scheme in storage_access:
            logger.debug("stage_out checking Staging provider {}".format(scheme))
            if scheme.can_stage_out(file):
                # globus_scheme._update_stage_out_local_path(file, executor, self.dfk)
                return scheme.stage_out(self, executor, file, app_fu)

        logger.debug("reached end of staging scheme list")
        # if we reach here, we haven't found a suitable staging mechanism
        raise ValueError("Executor {} cannot stage out file {}".format(executor, repr(file)))
