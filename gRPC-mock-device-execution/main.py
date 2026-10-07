"""IAG5 Python script: run ONE gNMI or gNOI command against a device. Single file, you pass every input with --set.

Actions
  gNMI: capabilities | get | update | replace | union_replace | delete
  gNOI: time | reboot | reboot_status | cancel_reboot
Output: one JSON object on stdout. Exit code 0 = ok, 1 = device/RPC error, 2 = bad input.
Needs: grpcio, protobuf>=7 (Python 3.10+). The gNMI 0.10 / gNOI protobuf definitions are embedded below, so no
generated stubs or other files are needed.
"""
import argparse
import base64
import json
import os
import sys
import zlib

import grpc
from google.protobuf import any_pb2, descriptor_pb2, descriptor_pool, duration_pb2, message_factory  # noqa: F401  (the *_pb2 imports register Google's own protos)

# --- embedded protobuf definitions (compressed FileDescriptorSet: gnmi_ext, gnmi, gnoi types/common/system) -------
_DESCRIPTORS = """
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
lnlYeRI+b/8fuR2Vpg==
"""

_pool = descriptor_pool.Default()
for _f in descriptor_pb2.FileDescriptorSet.FromString(zlib.decompress(base64.b64decode(_DESCRIPTORS))).file:
    _pool.AddSerializedFile(_f.SerializeToString())  # already in dependency order


def msg(name):
    """Message class by full name, e.g. msg('gnmi.SetRequest')."""
    return message_factory.GetMessageClass(_pool.FindMessageTypeByName(name))


def enum(name, value):
    return _pool.FindEnumTypeByName(name).values_by_name[value].number


# --- helpers ---------------------------------------------------------------------------------------------------
SET_ACTIONS = ("update", "replace", "union_replace")
ACTIONS = ("capabilities", "get") + SET_ACTIONS + ("delete", "time", "reboot", "reboot_status", "cancel_reboot")


def parse_path(s):
    """'/interfaces/interface[name=eth0]/config' -> gnmi.Path"""
    elems, buf, depth = [], "", 0
    for ch in s.strip() + "/":
        depth += (ch == "[") - (ch == "]")
        if ch == "/" and depth == 0:
            if buf:
                name, *keys = buf.replace("]", "").split("[")
                elems.append(msg("gnmi.PathElem")(name=name, key=dict(k.split("=", 1) for k in keys)))
            buf = ""
        else:
            buf += ch
    return msg("gnmi.Path")(elem=elems)


def path_str(p):
    return "/" + "/".join(e.name + "".join("[%s=%s]" % (k, e.key[k]) for k in sorted(e.key)) for e in p.elem)


def parse_value(v):
    try:
        return json.loads(v)
    except ValueError:
        return v  # bare text such as edge-01 is sent as a string


def decode(tv):
    kind = tv.WhichOneof("value")
    if kind in ("json_ietf_val", "json_val"):
        return json.loads(getattr(tv, kind).decode())
    return getattr(tv, kind) if kind else None


def updates_from(a):
    """List of (path, value) from --updates (JSON list of {path,value}) or --path + --value."""
    if a.updates:
        return [(u["path"], u["value"]) for u in json.loads(a.updates)]
    if a.path is not None and a.value is not None:
        return [(a.path, parse_value(a.value))]
    raise ValueError("%s needs --path and --value, or --updates" % a.action)


def run(a, ch, md):
    kw = dict(metadata=md, timeout=a.timeout)

    def call(service, method, req, resp_name):
        resp = msg(resp_name)
        return ch.unary_unary("/%s/%s" % (service, method), request_serializer=req.SerializeToString,
                              response_deserializer=resp.FromString)(req, **kw)

    act = a.action
    if act == "capabilities":
        r = call("gnmi.gNMI", "Capabilities", msg("gnmi.CapabilityRequest")(), "gnmi.CapabilityResponse")
        enc = _pool.FindEnumTypeByName("gnmi.Encoding").values_by_number
        return {"gnmi_version": r.gNMI_version, "models": [m.name for m in r.supported_models],
                "encodings": [enc[e].name for e in r.supported_encodings]}
    if act == "get":
        req = msg("gnmi.GetRequest")(path=[parse_path(a.path or "/")], encoding=enum("gnmi.Encoding", "JSON_IETF"),
                                     type=enum("gnmi.GetRequest.DataType", a.type.upper()))
        r = call("gnmi.gNMI", "Get", req, "gnmi.GetResponse")
        return {path_str(u.path): decode(u.val) for n in r.notification for u in n.update}
    if act in SET_ACTIONS or act == "delete":
        req = msg("gnmi.SetRequest")()
        if act == "delete":
            req.delete.append(parse_path(a.path))
        else:
            tv = lambda v: msg("gnmi.TypedValue")(json_ietf_val=json.dumps(v).encode())  # noqa: E731
            getattr(req, act).extend(msg("gnmi.Update")(path=parse_path(p), val=tv(v)) for p, v in updates_from(a))
        r = call("gnmi.gNMI", "Set", req, "gnmi.SetResponse")
        ops = _pool.FindEnumTypeByName("gnmi.UpdateResult.Operation").values_by_number
        return [{"op": ops[x.op].name, "path": path_str(x.path)} for x in r.response]
    if act == "time":
        r = call("gnoi.system.System", "Time", msg("gnoi.system.TimeRequest")(), "gnoi.system.TimeResponse")
        return {"time_ns": r.time}
    if act == "reboot":
        req = msg("gnoi.system.RebootRequest")(method=enum("gnoi.system.RebootMethod", "COLD"),
                                               delay=int(a.delay * 1e9), message="iag5 reboot")
        call("gnoi.system.System", "Reboot", req, "gnoi.system.RebootResponse")
        return {"reboot": "requested", "delay_seconds": a.delay}
    if act == "reboot_status":
        r = call("gnoi.system.System", "RebootStatus", msg("gnoi.system.RebootStatusRequest")(),
                 "gnoi.system.RebootStatusResponse")
        return {"active": r.active, "count": r.count}
    if act == "cancel_reboot":
        call("gnoi.system.System", "CancelReboot", msg("gnoi.system.CancelRebootRequest")(message="iag5 cancel"),
             "gnoi.system.CancelRebootResponse")
        return {"reboot": "cancelled"}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", required=True)
    ap.add_argument("--port", type=int, default=9339)
    ap.add_argument("--action", required=True, choices=ACTIONS)
    ap.add_argument("--username")
    ap.add_argument("--password", default=os.environ.get("GNMI_PASSWORD"))
    ap.add_argument("--tls", action="store_true", help="use TLS")
    ap.add_argument("--cafile", help="PEM CA/server cert (path relative to this script)")
    ap.add_argument("--servername", help="TLS server name override")
    ap.add_argument("--path", help="gNMI path, e.g. /interfaces/interface[name=eth0]/config (get default: /)")
    ap.add_argument("--value", help="JSON value (or bare text) for update/replace/union_replace")
    ap.add_argument("--updates", help='JSON list [{"path":"/system","value":{...}}, ...] (several updates at once)')
    ap.add_argument("--type", default="all", choices=["all", "config", "state"], help="for get")
    ap.add_argument("--delay", type=float, default=0, help="reboot delay in seconds")
    ap.add_argument("--timeout", type=float, default=10)
    a = ap.parse_args(argv)

    try:
        if a.action == "delete" and not a.path:
            raise ValueError("delete needs --path")
        if a.action in SET_ACTIONS:
            updates_from(a)  # validate before connecting
    except (ValueError, KeyError, TypeError) as e:
        print(json.dumps({"ok": False, "error": "bad input: %s" % e}))
        return 2

    target = "%s:%d" % (a.host, a.port)
    if a.tls:
        here = os.path.dirname(os.path.abspath(__file__))
        ca = open(os.path.join(here, a.cafile), "rb").read() if a.cafile else None
        opts = [("grpc.ssl_target_name_override", a.servername)] if a.servername else []
        channel = grpc.secure_channel(target, grpc.ssl_channel_credentials(root_certificates=ca), options=opts)
    else:
        channel = grpc.insecure_channel(target)
    md = [("username", a.username), ("password", a.password)] if a.username else None
    try:
        result = run(a, channel, md)
        print(json.dumps({"ok": True, "action": a.action, "target": target, "result": result}))
        return 0
    except grpc.RpcError as e:
        print(json.dumps({"ok": False, "action": a.action, "target": target,
                          "error": "%s: %s" % (e.code().name, e.details())}))
        return 1
    finally:
        channel.close()


if __name__ == "__main__":
    sys.exit(main())
