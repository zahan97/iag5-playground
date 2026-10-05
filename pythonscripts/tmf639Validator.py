#!/usr/bin/env python3
"""IAG5 service: check a device onboarding request against tmf639 payload policy.

Does one thing: takes the device details, runs every policy rule, and returns a
pass/fail checklist. No network connections and no third-party libraries.

Inputs arrive as --flag CLI args. IAG passes every decorator property, and unset
ones arrive as "", so each rule handles empty values itself.
Output is always a single JSON object on stdout.
"""
import argparse
import ipaddress
import json
import re

# ---------------------------------------------------------------------------
# Onboarding policy. Edit these to change the rules; no code changes needed.
# ---------------------------------------------------------------------------
POLICY = {
  "hostname_pattern": r"^lab-rtr-\d{2}$",
  "hostname_example": "lab-rtr-NN",
  "approved_roles": ["edge-router"],
  "model_platforms": {
    "csr1000v": ["cisco-ios"],
  },
  "mgmt_prefix_lengths": {"min": 24, "max": 30},
}


def check(name, passed, detail):
  return {"name": name, "result": "pass" if passed else "fail", "detail": detail}


def check_hostname(hostname):
  name = "hostname_convention"
  if not hostname:
    return check(name, False, "hostname not provided")
  if re.match(POLICY["hostname_pattern"], hostname):
    return check(name, True, f"'{hostname}' matches {POLICY['hostname_example']}")
  return check(name, False,
               f"'{hostname}' does not match {POLICY['hostname_example']}")


def check_mgmt_ip(mgmt_ip):
  name = "mgmt_ip_usable"
  if not mgmt_ip:
    return check(name, False, "mgmt_ip not provided")
  try:
    iface = ipaddress.ip_interface(mgmt_ip)
  except ValueError:
    return check(name, False,
                 f"'{mgmt_ip}' is not a valid address with prefix length, e.g. 10.10.10.13/24")

  ip = iface.ip
  net = iface.network
  limits = POLICY["mgmt_prefix_lengths"]

  if ip.version != 4:
    return check(name, False, f"{ip} is not IPv4")
  if "/" not in mgmt_ip:
    return check(name, False, f"'{mgmt_ip}' is missing a prefix length, e.g. /24")
  if not limits["min"] <= net.prefixlen <= limits["max"]:
    return check(name, False,
                 f"/{net.prefixlen} is outside the allowed /{limits['min']} to /{limits['max']}")
  if not ip.is_private:
    return check(name, False, f"{ip} is not a private address")
  if ip == net.network_address:
    return check(name, False, f"{ip} is the network address of {net}")
  if ip == net.broadcast_address:
    return check(name, False, f"{ip} is the broadcast address of {net}")
  gateway = next(net.hosts())
  if ip == gateway:
    return check(name, False, f"{ip} is reserved for the default gateway of {net}")
  return check(name, True, f"{ip} is a usable private address in {net}")


def check_platform(model, platform):
  name = "platform_matches_model"
  if not model or not platform:
    return check(name, False, "model and platform must both be provided")
  allowed = POLICY["model_platforms"].get(model)
  if allowed is None:
    return check(name, False, f"model '{model}' is not an approved model")
  if platform in allowed:
    return check(name, True, f"{model} allows {platform}")
  return check(name, False,
               f"{model} allows {', '.join(allowed)}, not '{platform}'")


def check_role(role):
  name = "role_approved"
  if not role:
    return check(name, False, "role not provided")
  if role in POLICY["approved_roles"]:
    return check(name, True, f"'{role}' is approved")
  return check(name, False,
               f"'{role}' is not approved; allowed: {', '.join(POLICY['approved_roles'])}")


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--hostname", default="")
  parser.add_argument("--mgmt_ip", default="")
  parser.add_argument("--model", default="")
  parser.add_argument("--platform", default="")
  parser.add_argument("--role", default="")
  args = parser.parse_args()

  # Run every rule so the caller sees all problems at once, not just the first
  checks = [
    check_hostname(args.hostname.strip()),
    check_mgmt_ip(args.mgmt_ip.strip()),
    check_platform(args.model.strip(), args.platform.strip()),
    check_role(args.role.strip()),
  ]
  failed = [c["name"] for c in checks if c["result"] == "fail"]

  print(json.dumps({
    "success": True,
    "passed": not failed,
    "failed_checks": failed,
    "checks": checks,
  }, indent=2))


if __name__ == "__main__":
  try:
    main()
  except Exception as exc:  # never crash; always hand JSON back to the workflow
    print(json.dumps({"success": False, "error": f"policy check error: {exc}"}))
