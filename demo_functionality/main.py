from demo_logic import load_rules, evaluate_status, load_assets, load_telemetry, evaluate_at_time, print_results_table

def main():
    """Main execution - evaluate pipeline segments at specific times."""
    
    # Load rules
    print("Loading rules...")
    thresholds = load_rules("rules.json")
    
    # Load data from database
    print("Loading assets from database...")
    assets = load_assets()
    print(f"  Loaded {len(assets)} segments")

    print("Loading telemetry from database...")
    telemetry = load_telemetry()
    print(f"  Loaded {len(telemetry)} readings")
    
    # Key times in the drift story
    times_to_check = [
        ("2026-01-18T10:05:00Z", "Before WARNING - SEG-02 should be OK"),
        ("2026-01-18T10:07:00Z", "After WARNING threshold - SEG-02 should be WARNING"),
        ("2026-01-18T10:12:00Z", "After CRITICAL threshold - SEG-02 should be CRITICAL"),
        ("2026-01-18T10:17:00Z", "After VIOLATION - SEG-02 should be VIOLATION"),
    ]
    
    for target_time, description in times_to_check:
        print(f"\n\n{description}")
        results = evaluate_at_time(target_time, assets, telemetry, thresholds, evaluate_status)
        print_results_table(results, target_time)

if __name__ == "__main__":
    main()