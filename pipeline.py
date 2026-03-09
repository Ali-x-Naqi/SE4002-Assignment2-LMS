import duckdb
import os
import logging
from pathlib import Path
import json
from datetime import datetime

# Set up logging matching enterprise standards
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

class EnvironmentalDataPipeline:
    """
    Main ETL and Analysis orchestration pipeline fulfilling the Big Data Handling constraint.
    Engineered with DuckDB to process out-of-core high-density telemetry before it reaches Pandas.
    """
    def __init__(self):
        self.con = duckdb.connect()
        self.raw_path = "data/simulated_env_2025.parquet"
        self.processed_dir = Path("data/processed")
        self.outputs_dir = Path("outputs")
        
        # Ensure directories exist
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        self.outputs_dir.mkdir(parents=True, exist_ok=True)

    def step_1_ingest_and_clean(self):
        """
        STEP 1: Ingest Big Data via DuckDB 
        Reads the high-density Parquet, filters corrupt/null telemetry natively,
        and saves the processed analytical dataset constraint-free.
        """
        logging.info("="*60)
        logging.info("STEP 1: Ingesting & Cleaning Telemetry Data via DuckDB")
        logging.info("="*60)
        
        if not os.path.exists(self.raw_path):
            logging.error(f"Raw data missing at {self.raw_path}. Run src/data_pipeline.py first.")
            return False
            
        # Natively copy cleanly processed data into processed dir
        query = f"""
        COPY (
            SELECT 
                timestamp, station_id, station_name, country, zone,
                pm25, pm10, no2, o3, temp, humidity
            FROM read_parquet('{self.raw_path}')
            WHERE pm25 IS NOT NULL AND station_id IS NOT NULL
        ) TO '{self.processed_dir}/master_data.parquet' (FORMAT PARQUET)
        """
        self.con.execute(query)
        logging.info(f"Cleaned dataset materialized at {self.processed_dir}/master_data.parquet")
        return True

    def step_2_aggregate_metrics(self):
        """
        STEP 2: Generate Core Aggregations
        Calculates heavy global report aggregates natively in DuckDB.
        """
        logging.info("="*60)
        logging.info("STEP 2: Generating Global Aggregations")
        logging.info("="*60)
        
        df_metrics = self.con.execute(f"""
            SELECT 
                COUNT(DISTINCT station_id) as total_sensors,
                COUNT(*) as valid_readings,
                COUNT(DISTINCT country) as countries_monitored,
                AVG(pm25) as global_mean_pm25,
                MAX(pm25) as absolute_max_pm25,
                SUM(CASE WHEN pm25 > 35 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as structural_violation_rate
            FROM read_parquet('{self.processed_dir}/master_data.parquet')
        """).fetchdf()
        
        # Save as JSON for auditable reporting
        report = df_metrics.to_dict(orient='records')[0]
        report['generated_at'] = datetime.utcnow().isoformat()
        report['query_engine'] = "DuckDB"
        
        with open(self.outputs_dir / "global_audit_report.json", "w") as f:
            json.dump(report, f, indent=4)
            
        logging.info(f"Global Audit Report compiled and exported to {self.outputs_dir}/global_audit_report.json")

    def run_full_pipeline(self):
        logging.info("\n" + "="*60)
        logging.info("URBAN ENVIRONMENTAL INTELLIGENCE - ETL PIPELINE")
        logging.info("="*60 + "\n")
        
        if self.step_1_ingest_and_clean():
            self.step_2_aggregate_metrics()
            
        logging.info("\n" + "="*60)
        logging.info("PIPELINE COMPLETE - READY FOR VISUALIZATION")
        logging.info("Execute dashboard: python -m streamlit run dashboard/app.py")
        self.con.close()

if __name__ == "__main__":
    pipeline = EnvironmentalDataPipeline()
    pipeline.run_full_pipeline()
