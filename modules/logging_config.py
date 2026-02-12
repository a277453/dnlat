import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from zipfile import ZipFile
from datetime import datetime


log_folder = Path(__file__).parent
main_log_file = log_folder / "app.log"

logger = logging.getLogger("app_logger")
logger.setLevel(logging.ERROR)
logger.propagate = False

formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] [%(name)s:%(module)s] %(message)s"
)

class CustomRotatingFileHandler(RotatingFileHandler):

    def rotation_filename(self, default_name):
        """
        Override default filenames like:
        app.log.1 → app1.log
        app.log.2 → app2.log
        """
        # Extract rotation number from default_name
        num = default_name.split(".")[-1]     # "1", "2", etc.
        return str(log_folder / f"app{num}.log")

    def doRollover(self):
        super().doRollover()  # Perform the normal rollover

        # Check if 5 custom rotated logs exist
        rotated_files = [log_folder / f"app{i}.log" for i in range(1, 6)]

        if all(f.exists() for f in rotated_files):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            zip_name = log_folder / f"App_Logs_{timestamp}.zip"

            with ZipFile(zip_name, 'w') as zipf:
                for f in rotated_files:
                    zipf.write(f, arcname=f.name)
                    f.unlink()  # delete original files

            logger.info("Zipped logs into %s and deleted app1.log–app5.log", zip_name)


# Use Custom Handler
file_handler = CustomRotatingFileHandler(
    main_log_file,
    maxBytes=5_000_000,
    backupCount=5
)
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
console_handler.setLevel(logging.INFO)

if not logger.handlers:
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

logger.info("Logger initialized at %s", main_log_file)
