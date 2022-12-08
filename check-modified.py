import json

with open("modified_scenes_20200929_20220930.json", "r") as f:
    data = json.load(f)
with open("modified_scenes_09292020_06302021.json", "r") as f:
    data2 = json.load(f)

files = []
for date in data.keys():
    for file in data[date]:
        files.append(file["filename"])

files2 = []
for date in data2.keys():
    for file in data2[date]:
        files2.append(file["filename"])

in_new_not_old = []

for file in files:
    if file not in files2:
        in_new_not_old.append(file)

in_old_not_new = []

for file in files2:
    if file not in files:
        in_old_not_new.append(file)
print(len(in_new_not_old), len(in_old_not_new))

print(in_new_not_old)
