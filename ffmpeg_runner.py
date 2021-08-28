import subprocess
import os
import asyncio
import glob

from time import time, sleep

import boto3
from icecream import ic
from decouple import config


ffmpeg = config("FFMPEG_LOC")
ffprobe = config("FFPROBE_LOC")


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
        self.current_video = None

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
        current_directory = os.getcwd()
        video_files = self.aws_bucket(client=client)
        print(f"video files {video_files} num {len(video_files)}")
        while True:
            for movie in video_files:
                print(f"movie - {movie}")
                # get duration
                # encode video
                # wait until time expired and delete segments
                self.aws_download_file(client, movie)
                times_searched = 0
                while times_searched < 3:
                    file_list = os.listdir(current_directory)
                    parent_dir = os.path.dirname(current_directory)
                    if movie in file_list:
                        self.current_video = movie
                        self.run_ffprobe(self.get_video_duration(input_file=movie))
                        # seconds = self.run_time.decode("utf-8").replace('FORMAT', '').replace('[', '').replace(']', '').replace('/', '')
                        print(f"search run time {float(self.run_time)}")
                        # self.run_parse(self.parse_video_duration(output_file=self.run_time.decode("utf-8") ))
                        self.runFFmpeg(self.buildFFmpegCommand(input_file=movie))
                        break
                    else:
                        times_searched += 1
                        print('movie not found')
                        sleep(2)

    def aws_download_file(self, client, movie):
        current_dir = os.getcwd()
        movie_file = client.download_file(
            'martystestspace',
            movie,
            f'{current_dir}/{movie}')
        return movie_file
            
    def grab_user_input(self, input_file):
        user_input_dict = dict()
        user_input_dict["input_file"] = input_file
        user_input_dict["output_file"] = config('OUTPUT_FILE_LOCATION')
        user_input_dict["video_codec"] = "libx264"
        user_input_dict["audio_codec"] = "aac"
        user_input_dict["audio_bitrate"] = "196k"
        user_input_dict["sample_rate"] = "44100"
        user_input_dict["encoding_speed"] = "fast"
        user_input_dict["speed"] = "21"
        user_input_dict["crf"] = "22"
        user_input_dict["frame_size"] = "1280x720"
        user_input_dict["hls_time"] = "5"
        user_input_dict["hls_list_size"] = "5"
        user_input_dict["start_number"] = "1"

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
            "-speed",
            final_user_input["speed"],
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

    def get_file_path_of_assets(self):
        raw_file_path = self.user_input['output_file'].split('/')
        final_file_path = ''
        for p in raw_file_path[1:-1]:
            final_file_path = final_file_path + '/' + p
        return final_file_path

    def remove_ts_files(self):
        print(f"user output file {self.user_input}")
        final_file_path = self.get_file_path_of_assets()
        for filename in os.listdir(final_file_path):
            file_path = os.path.join(final_file_path, filename)
            file_type = file_path.split('.')[-1:]
            try:
                print(f"file path {os.path.isfile(file_path)} type {file_type[0]} {file_type[0] == 'ts'}")
                if os.path.isfile(file_path) and file_type[0] == 'ts':
                    os.unlink(file_path)
                    print(f"delete {file_path}")
                # if os.path.isfile(file_path) and file_type[0] == 'm3u8':
                #     os.unlink(file_path)
                #     print(f"delete {file_path}")
            except Exception as e:
                return print(f'Failed to delete {file_path}. Reason: {e}')
        return print("Files removed successfully.")

    def remove_movie_file(self):
        final_file_path = os.getcwd()
        print(f"remove movie cwd {final_file_path}")
        for filename in os.listdir(final_file_path):
            file_path = os.path.join(final_file_path, filename)
            file_type = file_path.split('.')[-1:]
            try:
                print(f"file path {os.path.isfile(file_path)} type {file_type[0]} {file_type[0] == 'mp4'}")
                if os.path.isfile(file_path) and file_type[0] == 'mp4':
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
            self.remove_movie_file()
        else:
            print("There was an error running your FFmpeg script")

if __name__ == "__main__":
    from ffmpeg_runner import FfmpegRunner
    ru = FfmpegRunner()
    client = ru.aws_client()
    video_files = ru.aws_bucket(client=client)
    ru.retrieve_video_files(client)
