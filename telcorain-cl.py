import sys
import argparse
from warnings import simplefilter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from os import remove, listdir
from time import sleep
from concurrent.futures import ThreadPoolExecutor
import logging

from telcorain.database.influx_manager import influx_man
from telcorain.database.sql_manager import SqlManager
from telcorain.handlers.logging_handler import setup_init_logging, logger
from telcorain.handlers.writer import (
    RealtimeWriter,
    purge_raw_outputs,
    purge_web_outputs,
)
from telcorain.procedures.calculation import Calculation
from telcorain.procedures.utils.helpers import create_cp_dict, select_all_links

simplefilter(action="ignore", category=FutureWarning)


class TelcorainCLI:
    """Main class for TelcoRain CLI raingrid computation."""

    TIME_WINDOWS = {
        "1h": timedelta(hours=1),
        "3h": timedelta(hours=3),
        "6h": timedelta(hours=6),
        "12h": timedelta(hours=12),
        "1d": timedelta(days=1),
        "2d": timedelta(days=2),
        "7d": timedelta(days=7),
        "14d": timedelta(days=14),
        "30d": timedelta(days=30),
    }

    def __init__(
        self,
        config_path: str = "configs/config.ini",
        config_calc_path: str = "configs/config_calc.ini",
        config_db_path: str = "configs/config_db.ini",
    ):
        """Initialize CLI with configuration."""
        self.config = create_cp_dict(path=config_path, format=False)
        self.cp = create_cp_dict(path=config_calc_path, format=True)
        self.config_db = create_cp_dict(path=config_db_path, format=False)

        self.repetition_interval = int(self.config["setting"]["repetition_interval"])
        self.sleep_interval = int(self.config["setting"]["sleep_interval"])
        self.realtime_timewindow = self.TIME_WINDOWS.get(
            self.cp["realtime"]["realtime_timewindow"], timedelta(hours=1)
        ).total_seconds()

        self.sql_man = SqlManager(min_length=int(self.cp["cml"]["min_length"]))
        self.influx_man = influx_man

        setup_init_logging(logger, self.config["directories"]["logs"])
        sys.stdout.reconfigure(encoding="utf-8")

    def run(self):
        """Run the TelcoRain calculation in continuous loop."""
        try:
            self._print_init_log_info()
            links = self.sql_man.load_metadata()
            selected_links = select_all_links(links=links)

            calculation = Calculation(
                influx_man=self.influx_man,
                links=links,
                selection=selected_links,
                cp=self.cp,
                config=self.config,
            )

            while True:
                self._run_iteration(calculation)

        except KeyboardInterrupt:
            logger.info("Shutdown of the program...")

    def _run_iteration(self, calculation: Calculation):
        """Run a single calculation iteration."""
        logger.info("Starting new calculation...")

        # Get current time, next iteration time, and previous iteration time
        current_time, next_time, since_time = self._get_times()

        # Cleanup old data
        removed_files, kept_files = self._cleanup_old_files(current_time)
        deleted_rows = self.sql_man.delete_old_data(
            current_time,
            retention_window=self.TIME_WINDOWS.get(
                self.cp["realtime"]["realtime_timewindow"]
            ),
        )

        logger.info(
            f"Cleanup: Removed {removed_files} files, kept {kept_files}, "
            f"deleted {deleted_rows} DB rows"
        )

        # Run calculation
        calculation.run()

        # Write results
        writer = RealtimeWriter(
            sql_man=self.sql_man,
            influx_man=self.influx_man,
            skip_influx=self.cp["realtime"]["is_influx_write_skipped"],
            skip_sql=self.cp["realtime"]["is_sql_write_skipped"],
            since_time=since_time,
            cp=self.cp,
            config=self.config,
        )

        self._write_results(writer, calculation)
        self._sleep_until(next_time)

    def _write_results(self, writer: RealtimeWriter, calculation: Calculation):
        """Write calculation results to MariaDB."""
        self.sql_man.insert_realtime(
            self.realtime_timewindow,
            self.repetition_interval,
            self.cp["interp"]["interp_res"],
            self.cp["limits"]["x_min"],
            self.cp["limits"]["x_max"],
            self.cp["limits"]["y_min"],
            self.cp["limits"]["y_max"],
            self.config_db["http"]["http_server_address"],
            self.config_db["http"]["http_server_port"],
        )

        writer.push_results(
            rain_grids=calculation.rain_grids,
            x_grid=calculation.x_grid,
            y_grid=calculation.y_grid,
            calc_dataset=calculation.calc_data_steps,
        )

    def _sleep_until(self, target_time: datetime):
        """Sleep until target time with small intervals."""
        while datetime.now(tz=timezone.utc) < target_time:
            sleep(
                min(
                    self.sleep_interval,
                    (target_time - datetime.now(tz=timezone.utc)).total_seconds(),
                )
            )

    def _cleanup_old_files(self, current_time: datetime) -> tuple[int, int]:
        """Clean up old files from output directories."""
        retention_window = self.cp["realtime"]["realtime_timewindow"]
        threshold = current_time - self.TIME_WINDOWS.get(retention_window)

        raw_dir = Path(self.config["directories"]["outputs_raw"])
        web_dir = Path(self.config["directories"]["outputs_web"])

        files_to_delete = []
        files_to_keep = []

        for raw_file in raw_dir.glob("*.npy"):
            try:
                file_time = datetime.strptime(raw_file.stem, "%Y-%m-%d_%H%M").replace(
                    tzinfo=timezone.utc
                )
                web_file = web_dir / f"{raw_file.stem}.png"

                if file_time < threshold:
                    files_to_delete.append((raw_file, web_file))
                else:
                    files_to_keep.append(raw_file)
            except ValueError as e:
                logger.error(f"Skipping file {raw_file}: {e}")

        # Delete files in parallel
        with ThreadPoolExecutor() as executor:
            executor.map(self._delete_file_pair, files_to_delete)

        return len(files_to_delete), len(files_to_keep)

    def _delete_file_pair(self, file_pair: tuple[Path, Path]):
        """Delete a pair of raw and web files."""
        raw_file, web_file = file_pair
        try:
            if raw_file.exists():
                raw_file.unlink()
            if web_file.exists():
                web_file.unlink()
        except Exception as e:
            logger.error(f"Error deleting files: {e}")

    def _print_init_log_info(self):
        """Log initial configuration information."""
        config_info = [
            f"Logger level: {self.config['logging']['init_level']}",
            f"MariaDB: {self.config_db['mariadb']['address']}:{self.config_db['mariadb']['port']}",
            f"InfluxDB: {self.config_db['influx2']['url']}",
            f"Output folders - logs: {self.config['directories']['logs']}",
            f"web: {self.config['directories']['outputs_web']}",
            f"raw: {self.config['directories']['outputs_raw']}",
        ]

        calc_info = [
            f"Step: {self.cp['time']['step']}",
            f"IsMLPEnabled: {self.cp['wet_dry']['is_mlp_enabled']}",
            f"WAA Schleiss: {self.cp['waa']['waa_schleiss_val']}, {self.cp['waa']['waa_schleiss_tau']}",
            f"Interpolation: res {self.cp['interp']['interp_res']}, power {self.cp['interp']['idw_power']}",
            f"Realtime window: {self.cp['realtime']['realtime_timewindow']}",
            f"Optimization: {self.cp['realtime']['realtime_optimization']}",
        ]

        logger.info("[INFO] Global config settings: " + "; ".join(config_info))
        logger.info("[INFO] Calculation settings: " + "; ".join(calc_info))

    def _get_times(self) -> tuple[datetime, datetime, datetime]:
        """Get current, next, and since times for calculation."""
        current_time = datetime.now(tz=timezone.utc)
        return (
            current_time,
            current_time + timedelta(seconds=self.repetition_interval),
            current_time - timedelta(seconds=self.repetition_interval),
        )

    def wipeout_all_data(self):
        """Wipe all data from storage and databases."""
        logger.info("[DEVMODE] Erasing all calculation data...")
        self.sql_man.wipeout_realtime_tables()
        self.influx_man.run_wipeout_output_bucket()

        with ThreadPoolExecutor() as executor:
            executor.submit(purge_raw_outputs, config=self.config)
            executor.submit(purge_web_outputs, config=self.config)

        logger.info("[DEVMODE] Data erase complete.")


def main():
    """Main entry point for CLI."""
    parser = argparse.ArgumentParser(
        description="TelcoRain CLI for computing raingrids from CML data"
    )
    parser.add_argument("--run", action="store_true", help="Run the CLI calculation")
    parser.add_argument(
        "--wipe_all",
        action="store_true",
        help="Wipe all data from storage and databases",
    )

    args = parser.parse_args()
    cli = TelcorainCLI()

    if args.wipe_all:
        cli.wipeout_all_data()
    if args.run:
        cli.run()


if __name__ == "__main__":
    main()
