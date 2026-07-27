import os
import argparse

def get_arg():
    parser = argparse.ArgumentParser()
    parser.add_argument('--task_id', type=int, required=True)
    parser.add_argument('--path', default='scenario_data', type=str)
    parser.add_argument('--threads', default=1, type=int)
    args = parser.parse_args()
    return args.task_id, args.path, args.threads

if __name__ == '__main__':
    task_id, rpath, threads = get_arg()

    index = "baseline" if task_id == 0 else "scenario_{}".format(task_id)

    # Verify the input gdx exists before even calling GAMS
    gdx_path = "../{}/input_data/input_data_{}.gdx".format(rpath, index)
    if not os.path.exists(gdx_path):
        raise FileNotFoundError("Expected input file not found: {}".format(gdx_path))

    print("Running {} using {}".format(index, gdx_path))
    os.system(
        "gams ./Balmorel_ReadData_finish.gms --id={0} threads={2} "
        "> ../{1}/log_files/output_file_{0}.txt"
        .format(index, rpath, threads)
    )