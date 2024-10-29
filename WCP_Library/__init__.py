import os
import sys
from pathlib import Path


# Application Path
if getattr(sys, 'frozen', False):
    application_path = sys.executable + '-'
    application_path = Path(application_path).parent
else:
    application_path = Path(os.path.dirname(os.path.abspath(__file__)))
