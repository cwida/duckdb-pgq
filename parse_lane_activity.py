data = open('iterative_length.log', 'r').read()

vectors_split = data.split("#\n")

overall_finished_iteration = []
overall_finished_total = []
overall_discovered_cumulative = []
overall_discovered_iter = []

searches_in_batch = []
searches_in_vector = []


output_file = open("lane_activity.csv", "w")

output_file.write("vector,batch,iteration,discovered_cumulative,discovered_iteration,finished_iteration,finished_total\n")

for idx_vector, vector in enumerate(vectors_split):
    if vector == '':
        continue
    batch_split = vector.split("=\n")
    searches_in_vector.append(batch_split[0].strip().split()[6])
    for idx_batch, batch in enumerate(batch_split[1:]):
        iterations_split = batch.split('+\n')
        searches_in_batch = iterations_split[0].split()[4]
        for idx_iter, iteration in enumerate(iterations_split[1:]):
            lane_info = iteration.split("!\n")
            iteration_info = lane_info[0].split('\n')

            discovered_iter = float(iteration_info[2].split()[2].replace("%", ""))
            discovered_cumulative = float(iteration_info[4].split()[2].replace("%", ""))

            finished_searches_this_iteration = lane_info[-1].split("&\n")[-1].split("*\n")[0]
            finished_iteration = (int(finished_searches_this_iteration.split()[1]))

            total_finished_searches = lane_info[-1].split("*\n")[-1].split()[1]
            lanes = lane_info[-1].split('\n')
            output_file.write(f"{idx_vector},{idx_batch},{idx_iter},{discovered_cumulative},{discovered_iter},"
                              f"{finished_iteration},{total_finished_searches}\n")

