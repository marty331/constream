import subprocess
import os
import asyncio
import glob

from time import time, sleep

import boto3
from icecream import ic
from decouple import config


ffmpeg = "/usr/local/bin/ffmpeg"
ffprobe = "/usr/local/bin/ffprobe"


class FfmpegRunner:
    def __init__(self):
        self.user_input_dict = self.grab_user_input
        self.user_input = None
        self.run_time = 0
        # self.location = video_location
        self.aws_access_key_id = config('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = config('AWS_SECRET_ACCESS_KEY')
        self.region_name = config('REGION_NAME')
        self.endpoint_url = config('ENDPOINT_URL')

    def aws_client(self):
        session = boto3.session.Session()
        client = session.client('s3',
                            region_name=self.region_name,
                            endpoint_url=self.endpoint_url,
                            aws_secret_access_key=self.aws_secret_access_key,
                            aws_access_key_id=self.aws_access_key_id)
        return client

    def aws_bucket(self, client):
        resp = client.list_objects(Bucket='martystestspace')
        print(f"resp {resp}")
        results = []
        for obj in resp['Contents']:
            print(obj['Key'])
            results.append(obj['Key'])
        return results


    def retrieve_video_files(self, client):
        # print(f"location {self.location}")
        # video_files = [file for file in glob.glob(self.location + "*.mp4")]
        video_files = self.aws_bucket(client=client)
        print(f"video files {video_files} num {len(video_files)}")
        while True:
            for movie in video_files:
                print(f"movie - {movie}")
                # get duration
                # encode video
                # wait until time expired and delete segments
                movie_file = self.aws_download_file(client, movie)
                self.run_ffprobe(self.get_video_duration(input_file=movie_file))
                # seconds = self.run_time.decode("utf-8").replace('FORMAT', '').replace('[', '').replace(']', '').replace('/', '')
                print(f"search run time {float(self.run_time)}")
                # self.run_parse(self.parse_video_duration(output_file=self.run_time.decode("utf-8") ))
                self.runFFmpeg(self.buildFFmpegCommand(input_file=movie_file))

    async def aws_download_file(self, client, movie):
        movie_file = await client.download_file(
            'martystestspace',
            movie,
            f'/Users/marty331/PycharmProjects/ffmpeg_test/{movie}')
        return movie_file
            
    def grab_user_input(self, input_file):
        user_input_dict = dict()
        user_input_dict["input_file"] = input_file
        user_input_dict["output_file"] = "/Users/marty331/Movies/videos/second/index.m3u8"
        user_input_dict["video_codec"] = "libx264"
        user_input_dict["audio_codec"] = "aac"
        user_input_dict["audio_bitrate"] = "196k"
        user_input_dict["sample_rate"] = "44100"
        user_input_dict["encoding_speed"] = "fast"
        user_input_dict["crf"] = "22"
        user_input_dict["frame_size"] = "1280x720"
        user_input_dict["hls_time"] = "1"
        user_input_dict["hls_list_size"] = "3"
        user_input_dict["start_number"] = "1"
        # user_input_dict["output_file"] = filterInput("Output File (default = /Users/marty331/Movies/videos/second/index.m3u8): ", "/Users/marty331/Movies/videos/second/index.m3u8")
        # user_input_dict["video_codec"] = filterInput("Video Codec (default = libx264): ", "libx264")
        # user_input_dict["audio_codec"] = filterInput("Audio Codec (default = aac): ", "aac")
        # user_input_dict["audio_bitrate"] = filterInput("Audio Bitrate (default = 196k): ", "196k")
        # user_input_dict["sample_rate"] = filterInput("Sample Rate (default = 44100): ", "44100")
        # user_input_dict["encoding_speed"] = filterInput("Encoding Speed: (default = fast): ", "fast")
        # user_input_dict["crf"] = filterInput("Constant Rate Factor: (default = 22): ", "22")
        # user_input_dict["frame_size"] = filterInput("Frame Size (default = 1280x720): ", "1280x720")
        # user_input_dict["hls_time"] = filterInput("HLS Time (defualt = 10): ", "10")
        # user_input_dict["hls_list_size"] = filterInput("HLS List Size = (default = 20): ", "20")
        # user_input_dict["start_number"] = filterInput("Start Number = (defualt = 1): ", "1")

        self.user_input = user_input_dict
        return user_input_dict

    def get_video_duration(self, input_file):
        # ffprobe -i Walk_in_the_woods.mp4 -show_format -v quiet | sed -n 's/duration=//p'
        commands_list = [
            ffprobe,
            '-i',
            input_file,
            # '-show_format',
            '-v',
            'quiet',
            '-show_entries',
            'format=duration',
            '-hide_banner'
            # '|',
            # 'sed',
            # '-n',
            # 's/duration=//p'
        ]
        return commands_list

    def parse_video_duration(self, output_file: str):
        print(f'parse video output {output_file}')
        commands_list = [
            'grep',
            'duration',
            output_file
        ]
        return commands_list

    def buildFFmpegCommand(self, input_file):

        final_user_input = self.grab_user_input(input_file=input_file)

        commands_list = [
            ffmpeg,
            "-i",
            final_user_input["input_file"],
            "-c:v",
            final_user_input["video_codec"],
            "-preset",
            final_user_input["encoding_speed"],
            "-crf",
            final_user_input["crf"],
            "-sc_threshold",
            "0",
            "-s",
            final_user_input["frame_size"],
            "-c:a",
            final_user_input["audio_codec"],
            "-b:a",
            final_user_input["audio_bitrate"],
            "-ar",
            final_user_input["sample_rate"],
            "-pix_fmt",
            "yuv420p",
            "-fflags",
            "nobuffer",
            "-flags",
            "low_delay",
            "-f",
            "hls",
            "-hls_flags",
            "delete_segments",
            "-hls_time",
            final_user_input["hls_time"],
            "-hls_list_size",
            final_user_input["hls_list_size"],
            "-start_number",
            final_user_input["start_number"],
            final_user_input["output_file"]
        ]

        return commands_list

    def remove_ts_files(self):
        print(f"user output file {self.user_input}")
        raw_file_path = self.user_input['output_file'].split('/')
        final_file_path = ''
        for p in raw_file_path[1:-1]:
            final_file_path = final_file_path + '/' + p
        for filename in os.listdir(final_file_path):
            file_path = os.path.join(final_file_path, filename)
            file_type = file_path.split('.')[-1:]
            try:
                if os.path.isfile(file_path) and file_type == '.ts':
                    os.unlink(file_path)
                    print(f"delete {file_path}")
            except Exception as e:
                return print(f'Failed to delete {file_path}. Reason: {e}')
        return print("Files removed successfully.")

    def run_ffprobe(self, commands):
        print(f"run ffprobe {commands}")
        self.run_time = subprocess.run(commands, capture_output=True).stdout\
            .decode("utf-8").replace('FORMAT', '').replace('[', '').replace(']', '')\
            .replace('/', '').replace('/n', '').replace('duration=', '')
        print(f"run time {self.run_time}")

    def run_parse(self, commands):
        timer = subprocess.run(commands, capture_output=True)
        print(f"run parse {timer}")

    def runFFmpeg(self, commands):
        print(commands)
        start = time()
        if subprocess.run(commands).returncode == 0:
            print("FFmpeg Script Ran Successfully")
            end = time() - start

            print(f"process took {end }")
            wait_time = float(self.run_time) - end
            sleep(wait_time)
            self.remove_ts_files()
        else:
            print("There was an error running your FFmpeg script")
