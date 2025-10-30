# ==============================================
# MASTER START SCRIPT
# ==============================================

# start_demo.py
"""
Master script to start all producers for the demo
Runs clickstream and transaction producers in parallel

Usage:
    python start_demo.py
"""

import subprocess
import sys
import time
from datetime import datetime

def start_demo(duration=15, clickstream_rate=10, transaction_rate=1):
    """
    Start all producers for the demo
    
    Args:
        duration: Minutes to run
        clickstream_rate: Clickstream events per second
        transaction_rate: Transactions per minute
    """
    print("=" * 70)
    print("🚀 streammart DEMO - STARTING ALL PRODUCERS")
    print("=" * 70)
    print(f"⏱️  Duration: {duration} minutes")
    print(f"📊 Clickstream: {clickstream_rate} events/sec")
    print(f"💳 Transactions: {transaction_rate} per minute")
    print(f"🕐 Start time: {datetime.now().strftime('%H:%M:%S')}")
    print("=" * 70)
    print("\n✨ TIP: Start this 2-3 minutes before your demo presentation!")
    print("=" * 70)
    
    # Start clickstream producer
    print("\n🔄 Starting clickstream producer...")
    clickstream_process = subprocess.Popen([
        sys.executable,
        'clickstream_producer.py',
        '--duration', str(duration),
        '--rate', str(clickstream_rate)
    ])
    
    time.sleep(2)  # Give it a moment to start
    
    # Start transaction producer
    print("\n🔄 Starting transaction producer...")
    transaction_process = subprocess.Popen([
        sys.executable,
        'transaction_producer.py',
        '--duration', str(duration),
        '--rate', str(transaction_rate)
    ])
    
    print("\n" + "=" * 70)
    print("✅ ALL PRODUCERS RUNNING!")
    print("=" * 70)
    print("\n📋 DEMO CHECKLIST:")
    print("  □ NiFi flows processing? (Check NiFi UI)")
    print("  □ Flink jobs running? (Check Flink Dashboard)")
    print("  □ Dashboard updating? (Check CDV)")
    print("\n💡 Press Ctrl+C to stop all producers")
    print("=" * 70)
    
    try:
        # Wait for both to complete
        clickstream_process.wait()
        transaction_process.wait()
    except KeyboardInterrupt:
        print("\n\n⚠️  Stopping all producers...")
        clickstream_process.terminate()
        transaction_process.terminate()
        time.sleep(2)
        print("✅ All producers stopped")
    
    print("\n" + "=" * 70)
    print("🎬 DEMO PRODUCERS COMPLETED")
    print("=" * 70)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Start all demo producers')
    parser.add_argument('--duration', type=int, default=15,
                       help='Duration in minutes')
    parser.add_argument('--clickstream-rate', type=int, default=10,
                       help='Clickstream events per second')
    parser.add_argument('--transaction-rate', type=int, default=1,
                       help='Transactions per minute')
    
    args = parser.parse_args()
    
    start_demo(
        duration=args.duration,
        clickstream_rate=args.clickstream_rate,
        transaction_rate=args.transaction_rate
    )

