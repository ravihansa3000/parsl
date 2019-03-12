"""The following config uses threads say for local lightweight apps and IPP workers for
heavy weight applications.

The app decorator has a parameter `executors=[<list of executors>]` to specify the executor to which
apps should be directed.
"""
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.tests.utils import get_rundir

config = Config(
    executors=[
        HighThroughputExecutor(max_workers=1, label='local_htex_1'),
        HighThroughputExecutor(max_workers=1, label='local_htex_2'),
    ],
    run_dir=get_rundir()
)
