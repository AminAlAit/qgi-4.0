

def load_runner(indicators):
    for indicator in indicators:
        indicator.data.to_csv(r"/workspaces/qgi-4.0/data/" + indicator.id + ".csv")
