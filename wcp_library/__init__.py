import os
import sys
from pathlib import Path

# PyInstaller import
import pip_system_certs.wrapt_requests


# Application Path
if getattr(sys, 'frozen', False):
    application_path = sys.executable + '-'
    application_path = Path(application_path).parent
else:
    application_path = Path(os.environ['VIRTUAL_ENV']).parent
