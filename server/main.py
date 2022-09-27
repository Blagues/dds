import socket, random, copy, pickle

HOST = "127.0.0.1"  # Standard loopback interface address (localhost)
PORT = 65432  # Port to listen on (non-privileged ports are > 1023)

N_MACHINES = 1
URLS_PER_BATCH = 2

machines = []
urls_list = ['https://www.youtube.com/watch?v=ghGiv7YLC7Q', 'https://www.youtube.com/watch?v=32wDFCM7iSI']
urls_free = copy.deepcopy(urls_list)

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))

    # conncect with all machines
    while (len(machines) < N_MACHINES):
        s.listen()
        conn, addr = s.accept()
        machines += [conn]

    while True:
        if len(urls_list) == 0:  # stop when all urls have been successfully downloaded
            for conn in machines:
                conn.close()
            break

        urls_free = copy.deepcopy(urls_list)  # as we have removed all done urls, we can now copy urls_list

        # send urls to download to all machines
        for conn in machines:
            urls_to_do = []
            for i in range(URLS_PER_BATCH):
                url = random.choice(urls_free)
                urls_to_do += [url]
                urls_free.remove(url)

            data = bytearray(pickle.dumps(urls_to_do))

            conn.sendall(data)

        # receive from all machines
        for conn in machines:

            data = conn.recv(25_000_000)  # make sure that this is larger than the total size sent

            # load received data from bytes into list
            video_bytes = pickle.loads(data)

            for title, url, buf in video_bytes:
                audio_bytes = buf.read()

                with open("./out/" + title + ".mp3", "wb") as out_file:
                    out_file.write(audio_bytes)
                    out_file.close()
                urls_list.remove(url)
