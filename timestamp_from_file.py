'''
Takes a filename, start time in s, step size in s
and writes to stdout for each line a step from start time + line from file
'''
import sys


def main(args):
    file_name = args[1]
    start_time_in_s = float(args[2])
    step_in_s = float(args[3])

    start_time_in_ms = int(start_time_in_s * 1000)
    step_in_ms = int(step_in_s * 1000)

    with open(file_name) as f:
        for n, line in enumerate(f):
            new_time = start_time_in_ms + n * step_in_ms
            new_line = str(new_time) + "\t" + line
            sys.stdout.write(new_line)


if __name__ == '__main__':
    main(sys.argv)
