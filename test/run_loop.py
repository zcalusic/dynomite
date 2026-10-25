#!/usr/bin/env python3
import subprocess, time

def run_command(cmd):
    """Execute a shell command and return the output
       :param command: the command to be run and all of the arguments
       :returns: success_boolean, stderr
    """

    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    for line in iter(proc.stdout.readline, b''):
        print(">>> " + line.decode().rstrip())
    (stdout, stderr) = proc.communicate()
    return proc.returncode == 0, stderr

def main():
    cmd = "bash test/run_tests.sh -n"
    for i in range(0, 50):
        print("Running loop {}".format(i+1))
        success, stderr = run_command(cmd)
        if not success:
            for line in stderr.decode().splitlines():
                print(("--- " + line.rstrip()))
            break
        else:
            print("...........................Success\n")
        time.sleep(30)

if __name__ == "__main__":
        main()
