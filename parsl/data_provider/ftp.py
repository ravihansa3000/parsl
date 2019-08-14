import ftplib
import logging
import os

from parsl import python_app

from parsl.utils import RepresentationMixin
from parsl.data_provider.staging import Staging


logger = logging.getLogger(__name__)

# performs FTP staging as a separate parsl level task
# TODO: I'm expecting to also implement a Staging method that
# will stage tasks inside a task, for use where there is no
# shared file system.


class FTPSeparateTaskStaging(Staging, RepresentationMixin):

    def can_stage_in(self, file):
        logger.debug("FTPSeparateTaskStaging checking file {}".format(file.__repr__()))
        logger.debug("file has scheme {}".format(file.scheme))
        return file.scheme == 'ftp'

    def stage_in(self, dm, executor, file, parent_fut):
        working_dir = dm.dfk.executors[executor].working_dir
        stage_in_app = _ftp_stage_in_app(dm, executor=executor)
        app_fut = stage_in_app(working_dir, outputs=[file], staging_inhibit_output=True, parent_fut=parent_fut)
        return app_fut._outputs[0]


def _ftp_stage_in(working_dir, parent_fut=None, outputs=[], staging_inhibit_output=True):
    file = outputs[0]
    if working_dir:
        os.makedirs(working_dir, exist_ok=True)
        file.local_path = os.path.join(working_dir, file.filename)
    else:
        file.local_path = file.filename
    with open(file.local_path, 'wb') as f:
        ftp = ftplib.FTP(file.netloc)
        ftp.login()
        ftp.cwd(os.path.dirname(file.path))
        ftp.retrbinary('RETR {}'.format(file.filename), f.write)
        ftp.quit()


def _ftp_stage_in_app(dm, executor):
    return python_app(executors=[executor], data_flow_kernel=dm.dfk)(_ftp_stage_in)
