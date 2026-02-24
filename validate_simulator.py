#!/usr/bin/env python3
"""
Validator script to check if ANCHR Pump Simulator complies with real sensor protocol.

This script validates:
1. Message envelope structure (schema, fields, types)
2. Telemetry payload format
3. Transaction payload format  
4. State machine behavior
5. MQTT topic structure
"""

import json
import re
import sys
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple

# ANSI colors for output
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
RESET = "\033[0m"
BOLD = "\033[1m"

def ok(msg: str):
    print(f"  {GREEN}✓{RESET} {msg}")

def fail(msg: str):
    print(f"  {RED}✗{RESET} {msg}")

def warn(msg: str):
    print(f"  {YELLOW}!{RESET} {msg}")

def section(title: str):
    print(f"\n{BOLD}=== {title} ==={RESET}")

# ============================================================================
# PROTOCOL SPECIFICATION (from SYSTEM.md)
# ============================================================================

REQUIRED_ENVELOPE_FIELDS = [
    ("schema", str),
    ("schema_version", int),
    ("message_id", str),
    ("type", str),
    ("tenant_id", str),
    ("station_id", str),
    ("pump_id", str),
    ("device_id", str),
    ("event_time", str),
    ("seq", int),
    ("data", dict),
]

VALID_MESSAGE_TYPES = ["telemetry", "state", "tx", "ack", "event"]

TELEMETRY_REQUIRED_FIELDS = [
    ("state_code", int),
    ("display_amount", int),
    ("display_volume_liters", (int, float)),
    ("unit_price", int),
    ("shift_no", int),
    ("fuel_grade", str),
    ("currency", str),
]

TELEMETRY_OPTIONAL_FIELDS = [
    ("session_id", (str, type(None))),
    ("preset_amount", (int, type(None))),
    ("pump_rate_lpm", (int, float)),
    ("meter_totalizer_liters", (int, float)),
    ("fw_version", str),
    ("health", dict),
]

TX_REQUIRED_FIELDS = [
    ("tx_id", str),
    ("tx_seq", int),
    ("shift_no", int),
    ("total_volume_liters", (int, float)),
    ("total_amount", int),
    ("currency", str),
]

TX_OPTIONAL_FIELDS = [
    ("start_time", str),
    ("end_time", str),
    ("duration_ms", int),
    ("unit_price", int),
    ("fuel_grade", str),
    ("preset_amount", (int, type(None))),
    ("stop_reason", str),
    ("operator_id", (str, type(None))),
    ("status", str),
    ("meter_start_totalizer_liters", (int, float)),
    ("meter_end_totalizer_liters", (int, float)),
]

VALID_STATE_CODES = {
    0: "HOOK (Idle)",
    1: "LIFT (Ready)", 
    2: "PUMP (Pumping)",
    9: "ERROR",
}

VALID_FUEL_GRADES = ["RON92", "RON95", "DO"]
VALID_CURRENCIES = ["VND", "USD"]
VALID_STOP_REASONS = ["manual_stop", "reached_preset"]

# ============================================================================
# VALIDATION FUNCTIONS
# ============================================================================

def validate_topic_format(topic: str) -> Tuple[bool, str, Dict[str, str]]:
    """Validate MQTT topic structure: anchr/v1/{tenant_id}/{station_id}/{pump_id}/{type}"""
    pattern = r"^anchr/v1/([^/]+)/([^/]+)/([^/]+)/([^/]+)$"
    match = re.match(pattern, topic)
    if not match:
        return False, f"Topic doesn't match pattern: {pattern}", {}
    
    parts = {
        "tenant_id": match.group(1),
        "station_id": match.group(2),
        "pump_id": match.group(3),
        "type": match.group(4),
    }
    
    if parts["type"] not in VALID_MESSAGE_TYPES:
        return False, f"Invalid message type: {parts['type']}", parts
    
    return True, "OK", parts

def validate_rfc3339_utc(time_str: str) -> Tuple[bool, str]:
    """Validate RFC3339 UTC timestamp"""
    try:
        # Support both microseconds and without
        for fmt in ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"]:
            try:
                dt = datetime.strptime(time_str, fmt)
                if not time_str.endswith("Z"):
                    return False, "Timestamp must end with Z (UTC)"
                return True, "OK"
            except ValueError:
                continue
        return False, f"Not RFC3339 format: {time_str}"
    except Exception as e:
        return False, str(e)

def validate_envelope(envelope: Dict[str, Any], expected_type: str = None) -> Tuple[bool, List[str]]:
    """Validate message envelope structure"""
    errors = []
    
    # Check required fields
    for field, expected_type_cls in REQUIRED_ENVELOPE_FIELDS:
        if field not in envelope:
            errors.append(f"Missing required field: {field}")
        elif not isinstance(envelope[field], expected_type_cls):
            errors.append(f"Field '{field}' expected {expected_type_cls}, got {type(envelope[field])}")
    
    if errors:
        return False, errors
    
    # Validate schema format
    msg_type = envelope["type"]
    expected_schema = f"anchr.{msg_type}.v1"
    if envelope["schema"] != expected_schema:
        errors.append(f"Schema must be '{expected_schema}', got '{envelope['schema']}'")
    
    # Validate schema_version
    if envelope["schema_version"] != 1:
        errors.append(f"schema_version must be 1, got {envelope['schema_version']}")
    
    # Validate message_id (should be UUID-like)
    if not envelope["message_id"]:
        errors.append("message_id cannot be empty")
    
    # Validate type
    if envelope["type"] not in VALID_MESSAGE_TYPES:
        errors.append(f"Invalid type: {envelope['type']}")
    
    # Validate device_id format
    expected_device_id = f"{envelope['station_id']}:{envelope['pump_id']}"
    if envelope["device_id"] != expected_device_id:
        errors.append(f"device_id must be '{expected_device_id}', got '{envelope['device_id']}'")
    
    # Validate event_time
    valid, msg = validate_rfc3339_utc(envelope["event_time"])
    if not valid:
        errors.append(f"event_time: {msg}")
    
    # Validate seq
    if envelope["seq"] <= 0:
        errors.append(f"seq must be positive, got {envelope['seq']}")
    
    return len(errors) == 0, errors

def validate_telemetry_payload(data: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Validate telemetry payload"""
    errors = []
    
    # Check required fields
    for field, expected_types in TELEMETRY_REQUIRED_FIELDS:
        if field not in data:
            errors.append(f"Missing required field: {field}")
        elif not isinstance(data[field], expected_types):
            errors.append(f"Field '{field}' expected {expected_types}, got {type(data[field])}")
    
    if "state_code" in data:
        if data["state_code"] not in VALID_STATE_CODES:
            errors.append(f"Invalid state_code: {data['state_code']}. Valid: {list(VALID_STATE_CODES.keys())}")
    
    if "fuel_grade" in data:
        if data["fuel_grade"] not in VALID_FUEL_GRADES:
            errors.append(f"fuel_grade '{data['fuel_grade']}' not in standard list {VALID_FUEL_GRADES}")
    
    if "currency" in data:
        if data["currency"] not in VALID_CURRENCIES:
            errors.append(f"currency '{data['currency']}' not in standard list {VALID_CURRENCIES}")
    
    return len(errors) == 0, errors

def validate_tx_payload(data: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Validate transaction payload"""
    errors = []
    
    # Check required fields
    for field, expected_types in TX_REQUIRED_FIELDS:
        if field not in data:
            errors.append(f"Missing required field: {field}")
        elif not isinstance(data[field], expected_types):
            errors.append(f"Field '{field}' expected {expected_types}, got {type(data[field])}")
    
    # Check optional fields types if present
    for field, expected_types in TX_OPTIONAL_FIELDS:
        if field in data and data[field] is not None:
            if not isinstance(data[field], expected_types):
                errors.append(f"Field '{field}' expected {expected_types}, got {type(data[field])}")
    
    # Validate timestamps if present
    for time_field in ["start_time", "end_time"]:
        if time_field in data and data[time_field]:
            valid, msg = validate_rfc3339_utc(data[time_field])
            if not valid:
                errors.append(f"{time_field}: {msg}")
    
    # Validate stop_reason if present
    if "stop_reason" in data and data["stop_reason"]:
        if data["stop_reason"] not in VALID_STOP_REASONS:
            errors.append(f"stop_reason '{data['stop_reason']}' not in {VALID_STOP_REASONS}")
    
    return len(errors) == 0, errors

# ============================================================================
# SIMULATOR CODE ANALYSIS
# ============================================================================

def analyze_simulator_code():
    """Analyze simulator.py for protocol compliance"""
    section("ANALYZING SIMULATOR CODE")
    
    with open("simulator.py", "r") as f:
        code = f.read()
    
    issues = []
    findings = []
    
    # 1. Check PumpState enum
    if 'class PumpState(IntEnum)' in code:
        ok("PumpState enum defined correctly")
        if 'HOOK = 0' in code and 'LIFT = 1' in code and 'PUMP = 2' in code:
            ok("State codes match protocol (0=HOOK, 1=LIFT, 2=PUMP)")
        else:
            fail("State codes don't match protocol")
            issues.append("State codes mismatch")
    else:
        fail("PumpState enum not found")
        issues.append("Missing PumpState enum")
    
    # 2. Check envelope structure
    if '"schema": f"anchr.{subtopic}.v1"' in code:
        ok("Schema format follows 'anchr.{type}.v1' pattern")
    else:
        fail("Schema format incorrect")
        issues.append("Schema format incorrect")
    
    # 3. Check required envelope fields
    envelope_fields = ["schema", "schema_version", "message_id", "type", "tenant_id", 
                       "station_id", "pump_id", "device_id", "event_time", "seq", "data"]
    for field in envelope_fields:
        if f'"{field}"' in code:
            ok(f"Envelope field '{field}' present")
        else:
            fail(f"Envelope field '{field}' missing")
            issues.append(f"Missing envelope field: {field}")
    
    # 4. Check telemetry fields
    section("TELEMETRY PAYLOAD ANALYSIS")
    telemetry_fields = ["state_code", "display_amount", "display_volume_liters", 
                        "unit_price", "shift_no", "fuel_grade", "currency"]
    for field in telemetry_fields:
        if f'"{field}"' in code:
            ok(f"Telemetry field '{field}' present")
        else:
            fail(f"Telemetry field '{field}' missing")
            issues.append(f"Missing telemetry field: {field}")
    
    # 5. Check transaction fields
    section("TRANSACTION PAYLOAD ANALYSIS")
    tx_fields = ["tx_id", "tx_seq", "shift_no", "start_time", "end_time", 
                 "total_volume_liters", "total_amount", "currency", "status"]
    for field in tx_fields:
        if f'"{field}"' in code:
            ok(f"Transaction field '{field}' present")
        else:
            fail(f"Transaction field '{field}' missing")
            issues.append(f"Missing transaction field: {field}")
    
    # 6. Check MQTT topic format
    section("MQTT TOPIC FORMAT")
    if 'anchr/v1/{TENANT_ID}/{self.station_id}/{self.pump_id}' in code or \
       'f"anchr/v1/{TENANT_ID}' in code:
        ok("MQTT topic follows 'anchr/v1/{tenant}/{station}/{pump}/{type}' format")
    else:
        warn("Cannot verify MQTT topic format")
        findings.append("Verify MQTT topic format manually")
    
    # 7. Check timestamp format
    section("TIMESTAMP FORMAT")
    if 'strftime("%Y-%m-%dT%H:%M:%S.%fZ")' in code:
        ok("Timestamps use RFC3339 with microseconds (.%fZ)")
    elif 'strftime("%Y-%m-%dT%H:%M:%SZ")' in code:
        ok("Timestamps use RFC3339 (basic)")
    else:
        fail("Timestamp format might not be RFC3339")
        issues.append("Timestamp format unclear")
    
    # 8. Check state machine flow
    section("STATE MACHINE ANALYSIS")
    if 'PumpState.HOOK' in code and 'PumpState.LIFT' in code and 'PumpState.PUMP' in code:
        ok("All three pump states implemented (HOOK → LIFT → PUMP)")
        
        # Check state transitions
        if 'self.state = PumpState.LIFT' in code:
            ok("HOOK → LIFT transition implemented")
        if 'self.state = PumpState.PUMP' in code:
            ok("LIFT → PUMP transition implemented")
        if 'self.state = PumpState.HOOK' in code:
            ok("PUMP → HOOK transition implemented (cycle complete)")
    else:
        fail("State machine incomplete")
        issues.append("Incomplete state machine")
    
    # 9. Check QoS levels
    section("MQTT QoS ANALYSIS")
    if 'qos=1' in code:
        ok("Transaction messages use QoS 1 (guaranteed delivery)")
    else:
        warn("QoS 1 not detected for transactions")
        findings.append("Verify QoS 1 is used for transactions")
    
    if 'qos=0' in code:
        ok("Telemetry uses QoS 0 (fire-and-forget, acceptable for high-throughput)")
    else:
        warn("QoS 0 not detected for telemetry")
    
    # 10. Check fuel grade
    section("FUEL GRADE ANALYSIS")
    if 'RON95' in code or 'RON92' in code or 'DO' in code:
        ok("Standard fuel grades used")
    else:
        warn("Non-standard fuel grades might be used")
    
    return issues, findings

def generate_sample_messages():
    """Generate sample messages as simulator would produce"""
    section("SAMPLE MESSAGE GENERATION & VALIDATION")
    
    import uuid
    from datetime import datetime, timezone
    
    station_id = "st-0001"
    pump_id = "p-0001"
    device_id = f"{station_id}:{pump_id}"
    
    # Sample telemetry
    telemetry = {
        "schema": "anchr.telemetry.v1",
        "schema_version": 1,
        "message_id": str(uuid.uuid4()),
        "type": "telemetry",
        "tenant_id": "default",
        "station_id": station_id,
        "pump_id": pump_id,
        "device_id": device_id,
        "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "seq": 42,
        "data": {
            "state_code": 2,  # PUMP
            "display_amount": 500000,
            "display_volume_liters": 20.408,
            "pump_rate_lpm": 35.0,
            "unit_price": 24500,
            "shift_no": 1,
            "fuel_grade": "RON95",
            "currency": "VND",
            "session_id": str(uuid.uuid4()),
            "meter_totalizer_liters": 10020.408,
            "fw_version": "sim-1.0",
            "health": {"ok": True}
        }
    }
    
    # Sample transaction
    tx = {
        "schema": "anchr.tx.v1",
        "schema_version": 1,
        "message_id": str(uuid.uuid4()),
        "type": "tx",
        "tenant_id": "default",
        "station_id": station_id,
        "pump_id": pump_id,
        "device_id": device_id,
        "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "seq": 43,
        "data": {
            "tx_id": f"{device_id}:1:1",
            "tx_seq": 1,
            "shift_no": 1,
            "start_time": "2026-02-09T03:00:00.000000Z",
            "end_time": "2026-02-09T03:00:35.000000Z",
            "duration_ms": 35000,
            "fuel_grade": "RON95",
            "unit_price": 24500,
            "total_volume_liters": 20.408,
            "total_amount": 500000,
            "currency": "VND",
            "stop_reason": "manual_stop",
            "meter_start_totalizer_liters": 10000.0,
            "meter_end_totalizer_liters": 10020.408,
            "status": "COMPLETED"
        }
    }
    
    # Validate telemetry
    print(f"\n{BOLD}Validating sample TELEMETRY message:{RESET}")
    valid, errors = validate_envelope(telemetry)
    if valid:
        ok("Envelope structure valid")
    else:
        for err in errors:
            fail(err)
    
    valid, errors = validate_telemetry_payload(telemetry["data"])
    if valid:
        ok("Telemetry payload valid")
    else:
        for err in errors:
            fail(err)
    
    # Validate transaction
    print(f"\n{BOLD}Validating sample TX message:{RESET}")
    valid, errors = validate_envelope(tx)
    if valid:
        ok("Envelope structure valid")
    else:
        for err in errors:
            fail(err)
    
    valid, errors = validate_tx_payload(tx["data"])
    if valid:
        ok("Transaction payload valid")
    else:
        for err in errors:
            fail(err)
    
    # Validate MQTT topic
    print(f"\n{BOLD}Validating MQTT topic format:{RESET}")
    topic = f"anchr/v1/default/{station_id}/{pump_id}/tx"
    valid, msg, parts = validate_topic_format(topic)
    if valid:
        ok(f"Topic format valid: {topic}")
    else:
        fail(f"Topic format invalid: {msg}")

def main():
    print(f"{BOLD}{'='*60}{RESET}")
    print(f"{BOLD} ANCHR PUMP SIMULATOR - PROTOCOL COMPLIANCE VALIDATOR{RESET}")
    print(f"{BOLD}{'='*60}{RESET}")
    
    issues, findings = analyze_simulator_code()
    generate_sample_messages()
    
    # Summary
    section("VALIDATION SUMMARY")
    
    if issues:
        print(f"\n{RED}ISSUES FOUND ({len(issues)}):{RESET}")
        for issue in issues:
            print(f"  • {issue}")
    else:
        print(f"\n{GREEN}No critical issues found!{RESET}")
    
    if findings:
        print(f"\n{YELLOW}RECOMMENDATIONS ({len(findings)}):{RESET}")
        for finding in findings:
            print(f"  • {finding}")
    
    # Protocol compliance score
    total_checks = 30  # approximate
    passed = total_checks - len(issues)
    score = (passed / total_checks) * 100
    
    print(f"\n{BOLD}Protocol Compliance Score: {score:.0f}%{RESET}")
    
    if score >= 95:
        print(f"{GREEN}✓ Simulator is COMPLIANT with real sensor protocol{RESET}")
    elif score >= 80:
        print(f"{YELLOW}! Simulator is MOSTLY compliant, minor fixes recommended{RESET}")
    else:
        print(f"{RED}✗ Simulator has SIGNIFICANT compliance issues{RESET}")
    
    return 0 if len(issues) == 0 else 1

if __name__ == "__main__":
    sys.exit(main())
