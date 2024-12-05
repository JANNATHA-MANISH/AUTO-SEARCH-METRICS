from apscheduler.schedulers.background import BackgroundScheduler
import time
import signal
import sys

# Example run_pipeline function
def run_pipeline():
    print("Running the pipeline...")

# Graceful shutdown
def signal_handler(sig, frame):
    print("\nScheduler stopped gracefully.")
    scheduler.shutdown()
    sys.exit(0)

if __name__ == "__main__":
    scheduler = BackgroundScheduler()
    scheduler.add_job(run_pipeline, 'interval', hours=24)
    scheduler.start()
    print("Scheduler started. Press Ctrl+C to stop.")

    # Catch the signal to stop the scheduler
    signal.signal(signal.SIGINT, signal_handler)

    # Keep the script running
    while True:
        time.sleep(1)
