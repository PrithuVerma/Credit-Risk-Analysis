import time
from generate import generate_all
from transform import transform_all
from load import load_all

def run_pipeline():
    start = time.time()

    print("=" * 45)
    print("  CREDIT RISK ETL PIPELINE")
    print("=" * 45)

    # ── EXTRACT ───────────────────────────────────
    print("\n[1/3] EXTRACT — Generating raw data...")
    (
        officers_df,
        applicants_df,
        bureau_df,
        loans_df,
        repayments_df
    ) = generate_all()

    # ── TRANSFORM ─────────────────────────────────
    print("[2/3] TRANSFORM — Cleaning & engineering features...")
    (
        officers_df,
        applicants_df,
        bureau_df,
        loans_df,
        repayments_df,
        applicants_enriched,
        loans_enriched
    ) = transform_all(
        officers_df,
        applicants_df,
        bureau_df,
        loans_df,
        repayments_df
    )

    # ── LOAD ──────────────────────────────────────
    print("[3/3] LOAD — Inserting into PostgreSQL...")
    load_all(
        officers_df,
        applicants_df,
        bureau_df,
        loans_df,
        repayments_df
    )

    elapsed = round(time.time() - start, 2)

    print("\n" + "=" * 45)
    print(f"  PIPELINE COMPLETE — {elapsed}s")
    print("=" * 45)

    # Return enriched frames for optional EDA use
    return applicants_enriched, loans_enriched


if __name__ == '__main__':
    run_pipeline()