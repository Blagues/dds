import socket
import pickle
from pytube import YouTube
import io

HOST = "127.0.0.1"  # The server's hostname or IP address
PORT = 65432  # The port used by the server

SAVE_LOCALLY = False

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))

    while True:
        video_bytes = []

        data = s.recv(2048)
        if not data: break
        url_list = pickle.loads(data)

        for url in url_list:
            yt = YouTube(url)
            stream = yt.streams.filter(only_audio=True).first()

            buf = io.BytesIO()
            stream.stream_to_buffer(buf)
            buf.seek(0)

            if SAVE_LOCALLY:
                with open("./out/" + yt.title + ".mp3", "wb") as out_file:
                    out_file.write(audio_bytes)
                    out_file.close()

            video_bytes.append((yt.title, url, buf))

        vid_bin = bytearray(pickle.dumps(video_bytes))

        s.sendall(vid_bin)
