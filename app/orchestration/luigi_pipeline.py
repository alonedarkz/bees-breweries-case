from __future__ import annotations

from pathlib import Path

import luigi

from app.config import GOLD_DIR
from app.pipeline import run_pipeline


class BreweryMedallionPipeline(luigi.Task):
    run_id = luigi.Parameter(default="")

    def complete(self) -> bool:
        return False

    def output(self) -> luigi.LocalTarget:
        marker_name = f"{self.run_id or 'latest'}.done"
        return luigi.LocalTarget(str(Path(GOLD_DIR) / "_markers" / marker_name))

    def run(self) -> None:
        artifacts = run_pipeline(run_id=self.run_id or None)
        output_path = Path(self.output().path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            "\n".join(
                [
                    f"run_id={artifacts.run_id}",
                    f"bronze={artifacts.bronze_path}",
                    f"silver={artifacts.silver_path}",
                    f"gold={artifacts.gold_path}",
                ]
            ),
            encoding="utf-8",
        )
