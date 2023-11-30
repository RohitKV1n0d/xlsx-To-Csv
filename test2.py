import sys, os
import cv2
sys.path.append('src')
import pandas as pd
import logging
from vidgear.gears import CamGear
import queue
import threading
import time
from multiprocessing import Pool
\
logging.basicConfig(filename='my_program.log', level=logging.INFO)

from vision_engine.predictor import Predictor
from utility import get_project_dir

upload_dir = get_project_dir(project="steel_defects")

test_data_dir = os.path.dirname(__file__) + '/data'
model_dir = os.path.dirname(__file__) + '/../src/storage/models'

meta = {"shift": "A", "category": "PARALLEL_FLANGE_COLUMNS_UC_SERIES", "item": "UC 100 x 100 x 100"}
pred = Predictor(model_path=model_dir, output_path=upload_dir, meta = meta)


# Generate Video:
def framefetcher(source_path):
    stream = CamGear(source=source_path).start()
    frame_number = 0
    # fps = int(cap.get(cv2.CAP_PROP_FPS))
    fps = 30
    while True:
        frame = stream.read() 
        yield 0, frame_number, frame, fps
        # pred.predict_v1(fr, fps)
        # pred.start_prediction_and_counter_processes(fr, fps)



# Function to continuously read frames and put them into the queue
def record_frames(stream, frame_queue):
    while True:
        frame = stream.read()
        if frame is None:
            break
        frame_queue.put(frame)
    # Signal that there are no more frames to be added to the queue
    frame_queue.put(None)

# Generate Video:
def generate_video_frames(source_path):
    # Create a queue to store frames
    frame_queue = queue.Queue(maxsize=50)
    stream = CamGear(source=source_path).start()
    # Create and start a thread to handle frame recording
    record_thread = threading.Thread(target=record_frames, args=(stream, frame_queue))
    record_thread.start()
    frame_number = 0 
    fps = 30
    # Process frames from the queue
    while True:
        # Check if the queue is not empty
        if not frame_queue.empty():
            frame = frame_queue.get()
            frame_number += 1
            if frame is None:
                break
            yield 0, frame_number, frame, fps
        key = cv2.waitKey(1) & 0xFF
        if key == ord("q"):
            break
    cv2.destroyAllWindows()
    # Safely close video stream and release the video writer
    stream.stop()


if __name__=='__main__':
    logging.info('Starting my program...')
    web_cam = 0 
    video_path = test_data_dir + '/video.mp4'
    print(video_path)
    
    ip_camera_url_1 = 'rtsp://admin:admin123@192.168.1.108:554/cam/realmonitor?channel=1&subtype=0'
    ip_camera_url_2 = 'rtsp://admin:@Ombrulla1@192.168.1.13/1'
    ip_camera_url_3 = 'rtsp://admin:abcd1234@192.168.1.64/1'
    ls = []
    # pred = Predictor(model_path=model_dir, output_path=upload_dir, meta=meta)

    # pool = Pool(2)

    for sr, fid, fr, fps in framefetcher(video_path):
    
        start_time = time.time()
        pred.start_prediction_and_counter_processes(fr, fps)
        # pred.predict_v1(fr, fps)
        # pool.starmap(pred.start_prediction_and_counter_processes, [(fr, fps)])

        end_time = time.time()
        time_taken = end_time - start_time 
        print('time -----------------',time_taken)
        cv2.waitKey(1)
    logging.info('Program execution completed.')


