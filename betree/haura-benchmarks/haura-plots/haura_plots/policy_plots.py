def plot_delta(data, path):
    step_size = data.group_by(data['timestep'])['size'].sum()
    step_size
