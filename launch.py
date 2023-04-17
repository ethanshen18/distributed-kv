from threading import Event
import os
import sys
import subprocess
import time

N = int(sys.argv[1])
print(f"Spawning {N} processes")

ips = []
ports = []

exit = Event()

def quit(signo, _frame):
    print(f"received signal {signo}")
    exit.set()

def main():
    with open("servers.txt") as f:
        for l in f:
            args = l.split(":")
            ips.append(args[0].strip())
            ports.append(args[1].strip())

    open_processes = []

    for i in range(N):
        print(f"java -jar -Xmx512m {os.getcwd()}/target/CPEN431_2023_A12-1.0-SNAPSHOT-jar-with-dependencies.jar {ips[i]} {ports[i]}")
        open_processes.append(subprocess.Popen(["java", "-jar", "-Xmx512m", f"{os.getcwd()}/target/CPEN431_2023_A12-1.0-SNAPSHOT-jar-with-dependencies.jar", f"{ips[i]}", f"{ports[i]}"]))

    print("Ctrl-C to kill all processes")

    while not exit.is_set():
        exit.wait(120)
    # Kill all processes up to N times
    print("shutting down")
    for instance in open_processes:
        instance.terminate()

if __name__ == "__main__":

    import signal
    for sig in ("TERM", "HUP", "INT"):
        signal.signal(getattr(signal, f"SIG{sig}"), quit)

    main()
