import faulthandler
import sys
import traceback

faulthandler.enable()

try:
    from harness.cli import main
except Exception:
    traceback.print_exc()
    sys.exit(2)

sys.exit(main())
