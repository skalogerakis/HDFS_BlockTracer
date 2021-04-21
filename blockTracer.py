import subprocess
import sys, getopt
import re

# Execute in terminal using command
# python3 blockTracer.py /inputTester/

# Dictionnary to store the total size by ip node
hdfs_stats = {}


def result_printer():
    total_size = 0

    # Find the total size for sanity reasons
    for total in hdfs_stats.values():
        total_size += total

    print("Total Data Size = ", total_size)

    print("Sorted by value size")
    sorted_stats = sorted(hdfs_stats.items(), key=lambda item: item[1])

    # Print the sorted list by value size and
    for ip, size in sorted_stats:
        print("IP:", ip, "\tSIZE: ", size, "[%.1f" % (size / total_size * 100), "%]")


def main(argv):
    path = sys.argv[1]

    # Call the hdfs fsck command
    out = subprocess.Popen(['hdfs', 'fsck', path, '-files', '-blocks', '-locations'], stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT)
    stdout, stderr = out.communicate()
    fsck_line = stdout.decode("utf-8").split("\n")

    for line in fsck_line:
        if "DatanodeInfoWithStorage" in line:  # Keep only lines containing DatanodeWithStorage

            # Regex to get the length and replication factor of a HDFS block
            regex_len_rep = r'.* len=(\d*) Live_repl=(\d*).*'

            match_object_len_rep = re.match(regex_len_rep, line)
            # print(match_object_len_rep)
            block_size = int(match_object_len_rep.group(1))
            replication_factor = int(match_object_len_rep.group(2))
            # Regex to get all the ips on which a HDFS block is present
            regex_ips = r'.*' + '.*DatanodeInfoWithStorage\[([\w.]*)' * replication_factor
            print(regex_ips)
            match_object_ips = re.match(regex_ips, line)
            print(match_object_ips)
            for i in range(0, replication_factor):
                ip = match_object_ips.group(i + 1)

                # Calculate per ip size and get the corresponding load
                if ip in hdfs_stats.keys():
                    hdfs_stats[ip] += block_size
                else:
                    hdfs_stats[ip] = block_size

    result_printer()


if __name__ == "__main__":
    main(sys.argv[1:])
