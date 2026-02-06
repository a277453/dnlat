import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from zipfile import ZipFile
from datetime import datetime

# ==============================
# FRONTEND LOG CONFIG
# ==============================

log_folder = Path(__file__).parent
log_folder.mkdir(exist_ok=True)

main_log_file = log_folder / "streamlit_app.log"

logger = logging.getLogger("streamlit_logger")
logger.setLevel(logging.ERROR)
logger.propagate = False

formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] [STREAMLIT:%(module)s] %(message)s"
)

# ==============================
# CUSTOM ROTATION HANDLER
# ==============================

class StreamlitRotatingFileHandler(RotatingFileHandler):

    def rotation_filename(self, default_name):
        num = default_name.split(".")[-1]
        return str(log_folder / f"streamlit_app{num}.log")

    def doRollover(self):
        super().doRollover()

        rotated_files = [
            log_folder / f"streamlit_app{i}.log" for i in range(1, 6)
        ]

        if all(f.exists() for f in rotated_files):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            zip_name = log_folder / f"Streamlit_Logs_{timestamp}.zip"

            with ZipFile(zip_name, "w") as zipf:
                for f in rotated_files:
                    zipf.write(f, arcname=f.name)
                    f.unlink()

            logger.info(
                "Zipped frontend logs into %s and deleted streamlit_app1–5.log",
                zip_name
            )

# ==============================
# HANDLERS
# ==============================

file_handler = StreamlitRotatingFileHandler(
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

logger.info("Streamlit frontend logger initialized")
