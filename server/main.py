import socket, random, copy, pickle
import sys
import time
import select

HOST = "127.0.0.1"  # Standard loopback interface address (localhost)
PORT = 65432  # Port to listen on (non-privileged ports are > 1023)

N_MACHINES = 2
URLS_PER_BATCH = 2

machines = []
urls_list = ['https://www.youtube.com/watch?v=ghGiv7YLC7Q', 'https://www.youtube.com/watch?v=HyHNuVaZJ-k', 'https://www.youtube.com/watch?v=hTWKbfoikeg', 'https://www.youtube.com/watch?v=M1-YeqGynlw', 'https://www.youtube.com/watch?v=k85mRPqvMbE', 'https://www.youtube.com/watch?v=fregObNcHC8', 'https://www.youtube.com/watch?v=ghGiv7YLC7Q', 'https://www.youtube.com/watch?v=32wDFCM7iSI', 'https://www.youtube.com/watch?v=HyHNuVaZJ-k', 'https://www.youtube.com/watch?v=hTWKbfoikeg', 'https://www.youtube.com/watch?v=fregObNcHC8']
urls_list = list(set(urls_list))  # remove duplicates
n_to_do = len(urls_list)

start = 0

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))

    # conncect with all machines
    while (len(machines) < N_MACHINES):
        s.listen()
        conn, addr = s.accept()
        machines += [conn]

        print('Connected with', addr)

    start = time.time()

    print('\nAll clients connected - starting...\n')

    while True:
        print(n_to_do, 'urls left')
        if n_to_do == 0:  # stop when all urls have been successfully downloaded
            for conn in machines:
                conn.close()
            break

        # send urls to download to all machines
        if len(urls_list) > 0:
            for conn in machines:
                urls_to_do = []
                for i in range(URLS_PER_BATCH):
                    if len(urls_list) == 0: break
                    url = random.choice(urls_list)
                    urls_to_do += [url]
                    urls_list.remove(url)

                data = bytearray(pickle.dumps(urls_to_do))

                conn.sendall(data)

        # Get the list of sockets that are readable
        read_sockets, write_sockets, error_sockets = select.select(machines, [], [])

        # receive from all ready machines
        for conn in read_sockets:
            data = conn.recv(100_000_000)  # make sure that this is larger than the total size sent

            # load received data from bytes into list
            video_bytes = pickle.loads(data)

            for title, url, buf in video_bytes:
                audio_bytes = buf.read()

                with open("./out/" + title + ".mp3", "wb") as out_file:
                    out_file.write(audio_bytes)
                    out_file.close()

                n_to_do -= 1

print("Time taken: " + str(round(time.time() - start)) + " seconds")