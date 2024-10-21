import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from SALib.sample.morris import sample as morris
from SALib.sample.sobol import sample as sobol
from SALib.sample.latin import sample as lhc
from tabulate import tabulate
from utils import find_factors

class morris_sampler:
    def __init__(self, input="./scenario_creation/input/input_morris.csv", N=10, num_levels=4, optimal_trajectories=False, rng=None):
        self.input = pd.read_csv(input)
        self.N = N
        self.num_levels = num_levels
        self.optimal_trajectories = optimal_trajectories
        self.rng = rng
        if optimal_trajectories:
            self.num_trajectories = optimal_trajectories
        else:
            self.num_trajectories = N

        self.problem = {
        'num_vars': len(self.input["Parameter"]),
        'names': [name for name in self.input["Parameter"]],
        'bounds': [[lower, upper] for lower, upper in zip(self.input["Lower_bound"], self.input["Upper_bound"])],
        'dists': [dist for dist in self.input["Distribution_type"]]}

    def sample(self):
        self.samples = morris(self.problem, self.N, self.num_levels, optimal_trajectories=self.optimal_trajectories, seed=np.random.seed(self.rng))
        
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
            ax.hist(self.samples[:,i], bins=self.num_levels)
            ax.set_title(self.problem["names"][i])

        fig.savefig(output_file, bbox_inches = "tight")

class sobol_sampler:
    def __init__(self, input="./scenario_creation/input/input_morris.csv", N=8, rng=None):
        self.input = pd.read_csv(input)
        self.rng = rng
        self.N = N

        self.problem = {
        'num_vars': len(self.input["Parameter"]),
        'names': [name for name in self.input["Parameter"]],
        'bounds': [[lower, upper] for lower, upper in zip(self.input["Lower_bound"], self.input["Upper_bound"])],
        'dists': [dist for dist in self.input["Distribution_type"]]}

    def sample(self):
        self.samples = sobol(self.problem, self.N, seed=np.random.seed(self.rng))
        
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

#TO-DO
class lhc_sampler:
    def __init__(self, input="./scenario_creation/input/input_morris.csv", N=10, rng=None):
        self.input = pd.read_csv(input)
        self.rng = rng
        self.N = N

        self.problem = {
        'num_vars': len(self.input["Parameter"]),
        'names': [name for name in self.input["Parameter"]],
        'bounds': [[lower, upper, (lower+upper)/2, (upper-lower)/6] for lower, upper in zip(self.input["Lower_bound"], self.input["Upper_bound"])],
        'dists': [dist for dist in self.input["Distribution_type"]]}

    def sample(self):
        self.samples = lhc(self.problem, self.N, seed=np.random.seed(self.rng))
        
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
        