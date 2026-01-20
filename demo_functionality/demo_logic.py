import json

def load_rules(rules_file="rules.json"):
    """Load rules from JSON file."""
    with open(rules_file, 'r') as f:
        rules = json.load(f)
    return rules['rules']['maop_compliance']['thresholds']

def evaluate_status(pressure_psig, maop_psig, thresholds):
    """
    Determine status based on pressure relative to MAOP.
    
    Args:
        pressure_psig: Current pressure in psig
        maop_psig: Maximum Allowable Operating Pressure in psig
        thresholds: List of threshold rules from rules.json
        
    Returns:
        Status string: OK, WARNING, CRITICAL, or VIOLATION
    """
    if maop_psig <= 0:
        raise ValueError(f"Invalid MAOP: {maop_psig}")
    
    ratio = pressure_psig / maop_psig
    
    # Sort thresholds highest to lowest
    sorted_thresholds = sorted(thresholds, key=lambda x: x['threshold_ratio'], reverse=True)
    
    # Check each threshold from highest to lowest
    for threshold in sorted_thresholds:
        if ratio >= threshold['threshold_ratio']:
            return threshold['status']
    
    return "OK"

# Example usage
if __name__ == "__main__":
    # Load rules
    thresholds = load_rules("rules.json")
    
    # Test cases
    maop = 950.0  # MAOP in psig
    
    test_pressures = [
        (800, "Should be OK"),
        (855, "Should be WARNING (90%)"),
        (902, "Should be CRITICAL (95%)"),
        (950, "Should be VIOLATION (100%)"),
        (975, "Should be VIOLATION (over 100%)")
    ]
    
    print(f"MAOP: {maop} psig\n")
    print(f"{'Pressure':<12} {'Ratio':<12} {'Status':<12} {'Note':<30}")
    print("-" * 70)
    
    for pressure, note in test_pressures:
        status = evaluate_status(pressure, maop, thresholds)
        ratio = pressure / maop
        print(f"{pressure:<12} {ratio:<12.1%} {status:<12} {note:<30}")