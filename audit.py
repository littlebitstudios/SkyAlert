import yaml

with open("./data/config.yaml","r") as f:
    config = yaml.safe_load(f)
    
follow_watches = config["follow_watches"]
print("Active follow watches:", len(follow_watches))

user_watches = config["user_watches"]
unique_watchers = []
unique_subjects = []
watch_counts=[]

for watch in user_watches:
    if watch["subject-handle"] not in unique_subjects:
        unique_subjects.append(watch["subject-handle"])
        
    if watch["receiver-handle"] not in unique_watchers:
        unique_watchers.append(watch["receiver-handle"])
        watch_counts.append(1)
    else:
        watch_counts[unique_watchers.index(watch["receiver-handle"])] += 1

# Calculate the user with the most watches
if unique_watchers:
    max_watches = max(watch_counts)
    user_with_most_watches = unique_watchers[watch_counts.index(max_watches)]
    print("User with most watches: ", user_with_most_watches, "(with", max_watches, "watches)")
else:
    print("No watchers found.")
        
print("Unique watchers:", len(unique_watchers))
print("Unique subjects:", len(unique_subjects))