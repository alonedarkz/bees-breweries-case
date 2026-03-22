from app.pipeline import run_pipeline


def main() -> None:
    artifacts = run_pipeline()

    print("Pipeline concluida com sucesso.")
    print(f"run_id={artifacts.run_id}")
    print(f"bronze={artifacts.bronze_path}")
    print(f"silver={artifacts.silver_path}")
    print(f"gold={artifacts.gold_path}")
    print(f"monitoring={artifacts.monitoring_path}")


if __name__ == "__main__":
    main()
