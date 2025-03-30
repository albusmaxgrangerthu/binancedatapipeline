# app/src/scheduler.py
from apscheduler.schedulers.blocking import BlockingScheduler
import logging
import sys
import os
from datetime import datetime
import requests
from pathlib import Path
from crypto_data_pipeline import *

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('/app/logs/pipeline.log')
    ]
)
logger = logging.getLogger(__name__)

class TelegramNotifier:
    def __init__(self):
        self.token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = os.getenv('TELEGRAM_CHAT_ID')
        
        if not self.token or not self.chat_id:
            logger.warning("Telegram credentials not found in environment variables")
            
    def send_message(self, message: str, df: pd.DataFrame = None):
        """
        Send message via Telegram
        Args:
            message: The main message text
            df: Optional DataFrame to append to message
        """
        if not self.token or not self.chat_id:
            logger.error("Telegram credentials not configured")
            return
            
        try:
            # Format the complete message
            complete_message = message
            if df is not None and not df.empty:
                complete_message += "\n\n<pre>"  # Use HTML pre-formatting
                complete_message += df.to_string(index=False)
                complete_message += "</pre>"
                
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            payload = {
                "chat_id": self.chat_id,
                "text": complete_message,
                "parse_mode": "HTML"
            }
            
            response = requests.post(url, json=payload)
            response.raise_for_status()
            logger.info("Telegram notification sent successfully")
            
        except Exception as e:
            logger.error(f"Failed to send Telegram notification: {e}")

def job():
    """Main job function that runs the update pipeline"""
    start_time = datetime.now()
    notifier = TelegramNotifier()
    
    try:
        logger.info("Starting update job...")
        
        # Initialize pipeline
        pipeline = CryptoDataPipeline(
            Path(os.getenv('DATABASE_DIR'))/'duckdb_crypto.db',
            os.getenv('BINANCE_API_KEY'),
            os.getenv('BINANCE_API_SECRET')
        )
        
        # Run update
        #pipeline.update_all()
        # Run timely updates (5-minute data)
        logger.info("\n****** Running 5-minute updates ******")
        pipeline.update_timely()

        # Get extreme cases
        result_df = pipeline.get_extreme_cases(interval=30, threshold_delta=-0.006, threshold_diff=1440)

        # Run hourly updates if it's the start of an hour
        if datetime.now().minute < 5:  # Run hourly updates in first 5 minutes of each hour
            logger.info("\n****** Running hourly updates ******")
            pipeline.update_hourly()
        
        # Calculate execution time
        execution_time = (datetime.now() - start_time).total_seconds()
        
        if not result_df.empty:
            time_diff = (datetime.now() - result_df.fundingTime_cn[0]).total_seconds()
            if time_diff < 900:
                # Prepare success message
                message = (
                    "✅ Pipeline Update Successful\n\n"
                    f"⏱️ Execution Time: {execution_time:.2f}s\n"
                    f"🔄 Updated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                )
            
                logger.info(message)
                notifier.send_message(message, result_df)
            else:
                message = (
                    "✅ Pipeline Update Successful\n\n"
                    f"⏱️ Execution Time: {execution_time:.2f}s\n"
                    f"🔄 Updated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"📊 Results: No extreme cases found"
                )
            
                logger.info(message)
                notifier.send_message(message)
        
    except Exception as e:
        # Prepare error message
        error_message = (
            "❌ Pipeline Update Failed\n\n"
            f"⚠️ Error: {str(e)}\n"
            f"⏱️ Failed after: {(datetime.now() - start_time).total_seconds():.2f}s\n"
            f"🕒 Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        
        logger.error(error_message)
        notifier.send_message(error_message)
        raise

if __name__ == "__main__":
    # Create scheduler
    scheduler = BlockingScheduler()
    
    # Add job to run every 5 minutes
    scheduler.add_job(
        job, 
        'interval', 
        minutes=5,
        next_run_time=datetime.now()  # Run immediately on start
    )
    
    try:
        logger.info("Starting scheduler...")
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped")
    except Exception as e:
        logger.error(f"Scheduler failed: {str(e)}")
        sys.exit(1)