import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from SALib.sample.morris import sample as morris
from SALib.sample.sobol import sample as sobol
from SALib.sample.latin import sample as lhc
from tabulate import tabulate
from utils import find_factors

class sampler:
    def __init__(self, method, input="./input_params.csv", N=10, num_levels=4, rng=None):
        self.method = method
        self.input = pd.read_csv(input)
        self.N = N
        self.num_levels = num_levels
        self.rng = rng

        self.problem = {
        'num_vars': len(self.input["Parameter"]),
        'names': [name for name in self.input["Parameter"]],
        'bounds': [[lower, upper, (lower+upper)/2, (upper-lower)/6] for lower, upper in zip(self.input["Lower_bound"], self.input["Upper_bound"])],
        'dists': [dist for dist in self.input["Distribution_type"]]}

    def sample(self):
        if self.method == "Morris":
            self.samples = morris(self.problem, self.N, self.num_levels, seed=np.random.seed(self.rng))
        elif self.method == "Sobol":
            self.samples = sobol(self.problem, self.N, seed=np.random.seed(self.rng))
        elif self.method == "LHC":
            self.samples = lhc(self.problem, self.N, seed=np.random.seed(self.rng))
        else:   
            print("Invalid sampling method")
        
    def shuffle(self):
        np.random.seed(self.rng)
        idx = np.random.permutation(len(self.samples))
        self.samples = self.samples[idx]

    def write_Balmorel_files(self):
        with open("./scenarios.inc", 'w') as file:
            file.write('SET scenarios "Scenarios for the analysis"\n/Scenario1*Scenario{}/;'.format(len(self.samples)))

    def write_samples_to_inc(self, output="./scenario_creation/output/MorrisSampling_out.inc"):
        output = open(output, "w")
        header = []
        header.append("")
        for name in self.problem["names"]:
            header.append(name+" ")
        lines = []
        for i in range(self.samples.shape[0]):
            line = []
            line.append("Scenario{}".format(i+1))
            for j in range(self.samples.shape[1]):
                line.append(self.samples[i,j])
            lines.append(line)

        output.write(tabulate(lines, header, tablefmt="plain"))
        output.close()

    def save_samples(self, output_file):
        np.savetxt(output_file, self.samples, delimiter=',')

    def plot_samples(self, output_file):
        rows, cols = find_factors(self.problem["num_vars"])
        fig, axs = plt.subplots(rows, cols, figsize=(cols*3, rows*3))
        axs = axs.flatten()

        for i, ax in enumerate(axs):
            ax.hist(self.samples[:,i])
            ax.set_title(self.problem["names"][i])

        fig.savefig(output_file, bbox_inches = "tight")
        