from demo_logic import load_rules, evaluate_status

def main():
    """Main execution - test the MAOP evaluation logic."""
    
    # Path relative to where Python is running from
    rules_path = "demo_functionality/rules.json"
    
    # Load rules from JSON
    print(f"Loading rules from {rules_path}...")
    thresholds = load_rules(rules_path)
    print(f"Loaded {len(thresholds)} threshold rules\n")
    
    # Define test scenario
    maop = 950.0  # MAOP in psig
    
    print("=" * 80)
    print("ClearLine Pipeline - MAOP Compliance Evaluator")
    print("=" * 80)
    print(f"\nMAOP: {maop} psig\n")
    
    # Test cases showing drift from OK → WARNING → CRITICAL → VIOLATION
    test_pressures = [
        (750, "Normal operation"),
        (800, "Normal operation"),
        (850, "Approaching warning threshold"),
        (855, "At 90% - WARNING"),
        (880, "Still in WARNING range"),
        (902, "At 95% - CRITICAL"),
        (920, "Still in CRITICAL range"),
        (950, "At 100% - VIOLATION"),
        (975, "Over MAOP - VIOLATION"),
    ]
    
    print(f"{'Pressure (psig)':<18} {'% of MAOP':<15} {'Status':<15} {'Note':<30}")
    print("-" * 80)
    
    for pressure, note in test_pressures:
        status = evaluate_status(pressure, maop, thresholds)
        ratio = pressure / maop
        print(f"{pressure:<18} {ratio:<15.1%} {status:<15} {note:<30}")
    
    print("=" * 80)
    
    # Summary of thresholds
    print("\nThreshold Summary:")
    print("-" * 80)
    for threshold in sorted(thresholds, key=lambda x: x['threshold_ratio']):
        print(f"{threshold['status']:<15} {threshold['description']}")
    print("=" * 80)

if __name__ == "__main__":
    main()