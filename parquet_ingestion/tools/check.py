"""Static checks (run: python3 tools/check.py adf_old_json/pipeline) of ADF pipeline JSON: expression syntax (balanced, quoted), references, nesting, dependencies."""
import glob
import json
import re
import sys

errors = []


def segments(value):
    """Yield the ADF expression parts of a value: whole '@...' or each '@{...}' in an interpolated string."""
    if value.startswith("@") and not value.startswith("@{") and not value.startswith("@@"):
        yield value[1:]
        return
    i = 0
    while True:
        j = value.find("@{", i)
        if j < 0:
            return
        depth, k, inq = 0, j + 1, False
        while k < len(value):
            ch = value[k]
            if inq:
                if ch == "'":
                    if k + 1 < len(value) and value[k + 1] == "'":
                        k += 1
                    else:
                        inq = False
            elif ch == "'":
                inq = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    break
            k += 1
        if depth != 0:
            raise ValueError("unterminated @{ in " + value[j:j + 80])
        yield value[j + 2:k]
        i = k + 1


def check_expr(expr, where):
    depth, inq, k = 0, False, 0
    while k < len(expr):
        ch = expr[k]
        if inq:
            if ch == "'":
                if k + 1 < len(expr) and expr[k + 1] == "'":
                    k += 1
                else:
                    inq = False
        elif ch == "'":
            inq = True
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth < 0:
                errors.append(f"{where}: unbalanced ) in {expr[:100]}")
                return
        k += 1
    if inq:
        errors.append(f"{where}: unterminated string in {expr[:100]}")
    if depth:
        errors.append(f"{where}: unbalanced ( in {expr[:100]}")


def walk_values(obj, path, out):
    if isinstance(obj, dict):
        if obj.get("type") == "Expression" and "value" in obj:
            out.append((path, obj["value"]))
        for k, v in obj.items():
            if k in ("activities", "ifTrueActivities", "ifFalseActivities"):
                continue
            walk_values(v, path + "." + k, out)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            walk_values(v, f"{path}[{i}]", out)


CONTAINERS = {"ForEach", "IfCondition", "Until", "Switch"}


def check_pipeline(fn, all_names):
    pl = json.load(open(fn))
    props = pl["properties"]
    params = set(props.get("parameters", {}))
    optional = {k for k, v in props.get("parameters", {}).items() if "defaultValue" in v}
    variables = set(props.get("variables", {}))

    def acts_of(a):
        tp = a.get("typeProperties", {})
        return [(k, tp[k]) for k in ("activities", "ifTrueActivities", "ifFalseActivities") if k in tp]

    names = []

    def collect(acts):
        for a in acts:
            names.append(a["name"])
            for _, sub in acts_of(a):
                collect(sub)
    collect(props["activities"])
    dupes = {n for n in names if names.count(n) > 1}
    if dupes:
        errors.append(f"{fn}: duplicate activity names {dupes}")

    def visit(acts, scope, parent_type, in_foreach):
        local = {a["name"] for a in acts}
        for a in acts:
            where = f"{pl['name']}/{a['name']}"
            for d in a.get("dependsOn", []):
                if d["activity"] not in local:
                    errors.append(f"{where}: dependsOn {d['activity']} not in same container")
            if parent_type in ("IfCondition", "ForEach") and a["type"] in CONTAINERS and parent_type == "IfCondition":
                errors.append(f"{where}: {a['type']} nested inside IfCondition")
            if parent_type == "ForEach" and a["type"] == "ForEach":
                errors.append(f"{where}: ForEach nested inside ForEach")
            if in_foreach and a["type"] == "SetVariable":
                errors.append(f"{where}: SetVariable inside ForEach")
            if a["type"] == "ExecutePipeline":
                ref = a["typeProperties"]["pipeline"]["referenceName"]
                if ref in all_names:
                    child = json.load(open(all_names[ref]))["properties"].get("parameters", {})
                    given = set(a["typeProperties"].get("parameters", {}))
                    req = {k for k, v in child.items() if "defaultValue" not in v}
                    if req - given:
                        errors.append(f"{where}: missing child params {req - given}")
                    if given - set(child):
                        errors.append(f"{where}: unknown child params {given - set(child)}")
            # The Snowflake V2 connector refuses a Lookup query containing the file-transfer words PUT or COPY, even in a
            # comment (SnowflakeQueryStringNotSupported). Check the literal text; @{...} parts are resolved by ADF first.
            src = a.get("typeProperties", {}).get("source", {})
            if src.get("type") == "SnowflakeV2Source":
                literal = re.sub(r"@\{[^}]*\}", "", src["query"]["value"])
                for word in sorted(set(re.findall(r"\b(?:put|copy)\b", literal, re.I))):
                    errors.append(f"{where}: Snowflake query contains '{word}' (refused by the connector)")
            vals = []
            walk_values({k: v for k, v in a.items()}, where, vals)
            for path, v in vals:
                try:
                    segs = list(segments(v))
                except ValueError as e:
                    errors.append(f"{path}: {e}")
                    continue
                for seg in segs:
                    check_expr(seg, path)
                    for m in re.findall(r"activity\('([^']+)'\)", seg):
                        if m not in scope and m != a["name"]:
                            errors.append(f"{path}: activity('{m}') not visible")
                    for m in re.findall(r"variables\('([^']+)'\)", seg):
                        if m not in variables:
                            errors.append(f"{path}: unknown variable {m}")
                    for mo in re.finditer(r"pipeline\(\)\.parameters\.([A-Za-z_0-9]+)", seg):
                        m = mo.group(1)
                        if m not in params:
                            errors.append(f"{path}: unknown parameter {m}")
                        # A blank optional parameter arrives as null, not as its default: it must be read via coalesce.
                        elif m in optional and not seg[:mo.start()].endswith("coalesce("):
                            errors.append(f"{path}: optional parameter {m} used without coalesce(..., default)")
                    if "item()" in seg and not in_foreach and a["type"] != "Filter":
                        errors.append(f"{path}: item() outside ForEach")
            for k, sub in acts_of(a):
                visit(sub, scope | {x["name"] for x in sub}, a["type"], in_foreach or a["type"] == "ForEach")

    top = props["activities"]
    # activity() can reference any activity in enclosing scopes (ADF allows outer-scope references).
    visit(top, set(names), None, False)


files = sorted(glob.glob(sys.argv[1] + "/*.json"))
all_names = {json.load(open(f))["name"]: f for f in files}
for f in files:
    check_pipeline(f, all_names)
print("\n".join(errors) if errors else f"OK: {len(files)} pipelines, no issues")
