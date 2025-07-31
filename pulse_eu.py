"""
pulse_eu.py
-----------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    The main entry file for the model.
"""

import signal
from typing import Any

import pulse

from pulse.support.arguments import executionMode, ExecutionMode
from pulse.support.ui import time_function


def raise_keyboardinterrupt(sig: int, frame: Any) -> Any:
    """This function handles the SIGBREAK signal to enable the gui to cancel the model run."""
    raise KeyboardInterrupt


signal.signal(signal.SIGBREAK, raise_keyboardinterrupt)


@time_function("Total")
def start_model() -> None:
    """The main entry function."""
    model = pulse.PulseEUModel()
    model.run()


if __name__ == "__main__":
    match executionMode:
        case ExecutionMode.RUN:
            start_model()
        case ExecutionMode.SCHEDULER:
            pulse.start_scheduler()
        case ExecutionMode.INITIALIZE:
            pulse.initialize_data()
