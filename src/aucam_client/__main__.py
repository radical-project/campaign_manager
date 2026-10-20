"""``python -m src.aucam_client ...`` — same as the ``aucam-report`` command."""

import sys

from .cli import main

sys.exit(main())
