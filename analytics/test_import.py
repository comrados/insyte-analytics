import os
import importlib

print(__name__)
print(os.path.split(__file__)[1])

working_scripts_dir = "scripts"
dev_scripts_dir = "_in_development"


l = []

for scripts in [working_scripts_dir, dev_scripts_dir]:
    scripts_list = os.listdir(scripts)
    for script in scripts_list:
        name, ext = os.path.splitext(script)
        if name.startswith("analysis_"):
            try:
                #path = os.path.join("analytics/demand_response", script)
                l.append(importlib.import_module(scripts + "." + name))
                print("imported", name)
            except Exception as err:
                print("failed to import", name, err)
                pass

print(len(l))

d = {}

for i in l:
    d[getattr(i, "ANALYSIS_NAME")] = getattr(i, getattr(i, "CLASS_NAME"))

print(d)
