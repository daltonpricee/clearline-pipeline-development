"""
One-click demo setup: Reset everything and populate with fresh data.
"""
from reset_everything import reset_everything
from populate_demo_data import (
    populate_users,
    populate_assets,
    populate_sensors,
    populate_readings_with_story,
    populate_operator_acknowledgment
)

def setup_demo():
    """Complete reset and setup in one command."""
    print("\n" + "=" * 80)
    print(" " * 20 + "ClearLine Demo - Complete Setup")
    print("=" * 80 + "\n")

    # Step 1: Nuclear reset
    print("Step 1: Clearing database...")
    reset_everything()

    # Step 2: Populate fresh data
    print("\nStep 2: Populating fresh demo data...")
    populate_users()
    populate_assets()
    populate_sensors()
    populate_readings_with_story()
    populate_operator_acknowledgment()

    print("\n" + "=" * 80)
    print("✓ DEMO SETUP COMPLETE")
    print("=" * 80)
    print("\nDatabase Summary:")
    print("  • 4 pipeline segments (assets)")
    print("  • 4 pressure sensors")
    print("  • 3 users (operators/engineers)")
    print("  • 36 pressure readings with cryptographic hash chain")
    print("  • 1 operator acknowledgment (audit trail)")
    print("\nDrift Story Timeline (with Transient Filter Demo):")
    print("  10:00 - All segments normal")
    print("  10:02 - SEG-02 crosses 90% MAOP → WARNING")
    print("  10:03 - SEG-01 TRANSIENT SPIKE (96%) → FILTERED (not flagged)")
    print("  10:04 - SEG-01 returns to normal (proves spike was transient)")
    print("  10:07 - SEG-02 crosses 95% MAOP → CRITICAL (SUSTAINED)")
    print("  10:08 - Operator acknowledges")
    print("  10:09 - SEG-04 TRANSIENT SPIKE (97%) → FILTERED (not flagged)")
    print("  10:10 - SEG-04 returns to normal (proves spike was transient)")
    print("  10:12 - SEG-02 crosses 100% MAOP → VIOLATION (SUSTAINED)")
    print("\n  🧠 ClearLine's Smart Filter:")
    print("     - 2 transient spikes FILTERED (no nuisance alarms)")
    print("     - 1 sustained drift FLAGGED (real issue detected)")
    print("\nReady to Demo:")
    print("  python demo_hash_integrity.py")
    print("  python main.py")
    print("  streamlit run dashboard.py")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    setup_demo()
