hostname = input("Enter the hostname: ")
n = int(input("Enter number of nodes: "))

with open("servers.txt", "w") as f:
    for i in range(n):
        f.write("{}:{}\n".format(hostname, 8000 + i))