# app/src/scheduler.py
from apscheduler.schedulers.blocking import BlockingScheduler
import logging
import sys
import os
from datetime import datetime
import requests
from pathlib import Path
from utils import *
from utils_clickhouse import *
from config import *
from crypto_data_pipline_clickhouse import CryptoDataPipeline

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
    start_time = time.time()
    notifier = TelegramNotifier()
    
    try:
        logger.info("Starting update job...")
        
        # Initialize pipeline
        with connect_clickhouse(
            host=os.environ['CLICKHOUSE_HOST'],
            port=os.environ['CLICKHOUSE_PORT'],
            database=os.environ['CLICKHOUSE_DATABASE'],
            username=os.environ['CLICKHOUSE_USERNAME'],
            password=os.environ['CLICKHOUSE_PASSWORD']
        ) as con:
            logger.info("Database connection established")

            pipeline = CryptoDataPipeline(
                con=con,
                bn_api_key=os.environ['BINANCE_API_KEY'], 
                bn_api_secret=os.environ['BINANCE_API_SECRET']
            )
            logger.info("Pipeline initialized")
            
            # Run update
            #pipeline.update_all()
            # Run timely updates (5-minute data)
            logger.info("\n****** Running 1-hour updates ******")
            pipeline.update_all()

            message = (
                "✅ Pipeline Update Successful\n\n"
                f"⏱️ Execution Time: {(time.time() - start_time):.2f}s\n"
                f"🔄 Updated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            )
            
            logger.info(message)
            #notifier.send_message(message)
        
    except Exception as e:
        # Prepare error message
        error_message = (
            "❌ Pipeline Update Failed\n\n"
            f"⚠️ Error: {str(e)}\n"
            f"⏱️ Failed after: {(time.time() - start_time):.2f}s\n"
            f"🕒 Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        
        logger.error(error_message)
        #notifier.send_message(error_message)
        raise

if __name__ == "__main__":
    # Create scheduler
    scheduler = BlockingScheduler()
    
    now = datetime.now()
    run_immediate = now.minute >= update_minute
    next_run = now if run_immediate else now.replace(minute=update_minute, second=0, microsecond=0)
    
    # Add job to run at minute 58 of every hour
    job = scheduler.add_job(
        job, 
        'cron', 
        minute=f'{update_minute}',
        next_run_time=next_run
    )
    
    try:
        next_time = job.next_run_time
        if run_immediate:
            logger.info(f"Starting scheduler... Running immediately at: {now.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            logger.info(f"Starting scheduler... Current time: {now.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"Next run scheduled for: {next_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped")
    except Exception as e:
        logger.error(f"Scheduler failed: {str(e)}")
        sys.exit(1)