import logging
import os

from parsl.utils import RepresentationMixin
from parsl.data_provider.staging import Staging

logger = logging.getLogger(__name__)


class FileArgsStaging(Staging, RepresentationMixin):
    """A staging provider than can stage in local files
    by sending the file alongside the app function call. This
    provider probably only works for small files. It does not
    need a shared file system on the executor side."""

    def can_stage_in(self, file):
        logger.debug("FileArgsStaging checking file {}".format(file.__repr__()))
        logger.debug("file has scheme {}".format(file.scheme))
        return file.scheme == 'file'

    def can_stage_out(self, file):
        logger.debug("FileArgsStaging checking file {} for stageout".format(file.__repr__()))
        logger.debug("file has scheme {}".format(file.scheme))
        return file.scheme == 'file'

    def replace_task(self, dm, executor, file, func):
        with open(file.filename, 'rb') as fh:
            content = fh.read()
        working_dir = dm.dfk.executors[executor].working_dir
        return in_task_transfer_wrapper(func, file, working_dir, content)


def in_task_transfer_wrapper(func, file, working_dir, content):
    """Write out the named file before invoking the wrapped function."""
    def wrapper(*args, **kwargs):
        if working_dir:
            os.makedirs(working_dir, exist_ok=True)
            file.local_path = os.path.join(working_dir, file.filename)
        else:
            file.local_path = file.filename
        with open(file.local_path, 'wb') as fh:
            fh.write(content)

        result = func(*args, **kwargs)
        return result
    return wrapper
