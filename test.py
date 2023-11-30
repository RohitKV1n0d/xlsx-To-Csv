import os
import cv2
import pandas as pd
import datetime
from ultralytics import YOLO
from multiprocessing import Process # commt
from multiprocessing import Pool
import csv
from vision_engine.counter import Counter

class Predictor:
    NON_DETECT_THRESHOLD = 10

    def __init__(self, model_path: str, output_path: str, meta=None):
        if meta is None:
            meta = {}
        self.shift = meta.get('shift', '')
        self.category = meta.get('category', '')
        self.item = meta.get('item', '')
        self.model = YOLO(os.path.join(model_path, 'detector','ub_wyt.pt'))
        self.counter = Counter(model_path)
        self.names = self.model.names
        self.output_path = output_path
        self.current_date = datetime.datetime.now().strftime("%Y%m%d")
        self.non_detect = 0
        self.cls = None
        self.item_id = 0
        self.item_i_id = 0
        self.defect_id = 0
        self.frame_id = 0
        self.pre_cls = None
        self.pre_item_id = 0
        self.frame_start = 0
        self.frame_end = 0 
        self.label_buffer = []
        self.label_count = 0  
        self.draw_cls = self.draw_box = self.draw_frame = None
        self.initialize_csv()

        

    def initialize_csv(self):
        self.csv_file_path = os.path.join(self.output_path, 'dataset.csv')
        if os.path.exists(self.csv_file_path):
            df_data = pd.read_csv(self.csv_file_path)
            self.frame_id = df_data['sl_no'].max()
            self.defect_id = df_data['track_id'].max()
            self.item_i_id = df_data['item_id'].max()
        else:
            self.defect_id = 0
            self.frame_id = 0
            self.item_i_id = 0

    def calculate_defect_location(self, frame_start, frame_defect):
        object_speed = 2
        if frame_start is not None and frame_defect is not None:
            time_start = frame_start / self.fps
            time_defect = frame_defect / self.fps
            time_difference = time_defect - time_start
            distance_traveled = object_speed * time_difference
            return distance_traveled
            
    def draw_bounding_boxes_with_class(self,frame, box, cls_l):
        if cls_l is not None:
            cls_l = cls_l
            color = (0, 0, 255)
            x, y, w, h = box
            start = (int(x), int(y))
            end = (int(x + w), int(y + h))
            frame = cv2.rectangle(frame, start, end, color, 2)
            frame = cv2.putText(frame, cls_l, (int(x), int(y) - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.5, color, 2)
        frame = cv2.resize(frame, (640, 480))
        cv2.imshow("Bounded Image", frame)
        cv2.waitKey(1)
    

    def predict_v1(self, frame, fps):
        self.fps = fps
    # def predict_v1(self, frame, fps=None):
    #     if fps:
    #         self.fps = fps
    #     else:
    #         self.fps = 0

        self.draw_frame = frame

        
        # item_id = self.counter.count(frame)
        # if item_id != 0:
        #     if self.pre_item_id is not None and self.pre_item_id != item_id:
        #         self.pre_item_id = item_id
        #         self.item_i_id += 1
        #         self.item_id = self.item_i_id
        #         self.frame_start = self.frame_id
        #     if item_id == 0:
        #         self.item_id = 0
    
        results = self.model.track(frame, verbose=False, tracker="bytetrack.yaml", conf=0.5, iou=0.5)
        pred_list = []
        for r in results:
            cls_list = [self.names.get(i) for i in r.boxes.cls.cpu().numpy().tolist()] if r.boxes.cls is not None else []
            
            conf_list = [round(conf * 100) for conf in r.boxes.conf.cpu().numpy().tolist()] if r.boxes.conf is not None else []
            box_list = r.boxes.xywh.cpu().numpy().tolist() if r.boxes.xywh is not None else []
            for cls, score, box in zip(cls_list, conf_list, box_list):
                
                if score <= 0:
                    self.draw_cls = None
                    self.draw_box = None
                    self.cls = None
                else:
                    self.draw_cls = cls.upper()
                    self.draw_box = box
                    self.cls = cls
                self.frame_id += 1
                
                if self.cls is not None and self.non_detect > self.NON_DETECT_THRESHOLD:
                    self.defect_id += 1
                    self.frame_end = self.frame_id
                    self.non_detect = 0
                    self.pre_cls = self.cls
                    self.cls = None
                    self.item_id = self.item_i_id
                if self.pre_cls != self.cls and self.cls is not None and self.pre_cls is not None:
                    self.defect_id += 1
                    self.frame_end = self.frame_id
                    self.cls = None
                    self.pre_cls = None
                    self.item_id = self.item_i_id
                if self.cls is not None:
                    def_dis = self.calculate_defect_location(self.frame_start, self.frame_end)
                else:
                    def_dis = 0
                self.non_detect = 0
                pred_list.append({
                    'shift': self.shift,
                    'sl_no': self.frame_id,
                    'item_id': self.item_id,
                    'track_id': self.defect_id,
                    'label': cls.upper(),
                    'score': score,
                    'x': box[0],
                    'y': box[1],
                    'w': box[2],
                    'h': box[3],
                    'area': (box[2] * box[3]) * 0.3,
                    'item_name': self.item,
                    'category': self.category,
                    'location': def_dis,
                    'date': self.current_date,
                    'time': datetime.datetime.now().strftime("%H%M%S")
                })

        if not pred_list:
            self.draw_cls = None
            self.draw_box = None
            self.frame_id += 1
            pred_list.append({
                'shift': self.shift,
                'sl_no': self.frame_id,
                'item_id': self.item_id,
                'track_id': '',
                'label': '',
                'score': 0,
                'x': 0,
                'y': 0,
                'w': 0,
                'h': 0,
                'area': 0,
                'item_name': self.item,
                'category': self.category,
                'location': 0,
                'date': self.current_date,
                'time': datetime.datetime.now().strftime("%H%M%S")
            })
            self.non_detect += 1
            self.cls = None
        self.draw_bounding_boxes_with_class(self.draw_frame, self.draw_box, self.draw_cls)
        if os.path.exists(self.csv_file_path):
            df = pd.DataFrame(pred_list)
            df.to_csv(self.csv_file_path, mode='a', header=False, index=False)
        else:
            df = pd.DataFrame(pred_list)
            df.to_csv(self.csv_file_path, index=False)

    def count_1(self, frame):
        item_id = self.counter.count(frame)
        if item_id != 0:
            if self.pre_item_id is not None and self.pre_item_id != item_id:
                self.pre_item_id = item_id
                self.item_i_id += 1
                self.item_id = self.item_i_id
                self.frame_start = self.frame_id
            if item_id == 0:
                self.item_id = 0
                
    def start_prediction_and_counter_processes(self, frame, fps):
        self.frame_id += 1

        pool = Pool(2)
        pool.starmap(pred.start_prediction_and_counter_processes, [(fr, fps)])
        pool.close()
        pool.join()
    
    def start_prediction_and_counter_processes(self, frame, fps):

        self.frame_id += 1

        prediction_process = Process(target=self.predict_v1, args=(frame, fps))
        counter_process = Process(target=self.count_1, args=(frame,))

        prediction_process.start()
        counter_process.start()

        prediction_process.join()
        counter_process.join()
        
###########################################################################################
    def write_to_csv(self, pred_result, csv_filename): 
        # Set a limit for continuous predictions  
        countinous_pred_limit = 4  # modify if needed
        with open(csv_filename, mode='a', newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=pred_result[0].keys())
            # If the CSV file is empty, write the header row
            if csv_file.tell() == 0:
                writer.writeheader()
            for data in pred_result:
                if 'label' in data and data['label']:
                    if len(self.label_buffer) == 0:
                        self.label_buffer.append(data['label'])
                        self.label_count = 1
                    elif self.label_buffer[-1] == data['label']:
                        self.label_buffer.append(data['label'])
                        self.label_count += 1
                    else:
                        self.label_buffer = [data['label']]
                        self.label_count = 1
                    if self.label_count == countinous_pred_limit:
                        for pred_data in pred_result:
                            writer.writerow(pred_data)
                        self.label_buffer = []
                        self.label_count = 0  
    # Function to predict the defect and write the data only when the label repeats in consecutive frames:
    def predict_v2(self, frame, fps):
        self.fps = fps
        self.draw_frame = frame
        item_id = self.counter.count(frame)
        if item_id != 0 and self.pre_item_id is not None and self.pre_item_id != item_id:
            self.pre_item_id = item_id
            self.item_i_id = self.item_i_id + 1 
            self.item_id = self.item_i_id
            # print('hi',self.item_i_id)
            self.frame_start = self.frame_id
        if item_id == 0:
            self.item_id = 0
        results = self.model.track(frame, verbose=False, tracker="bytetrack.yaml") #conf= 0.7
        pred_list = []
        for r in results:
            cls_list = [self.names.get(i) for i in r.boxes.cls.cpu().numpy().tolist()] if r.boxes.cls is not None else []
            print(cls_list)
            conf_list = [round(conf * 100) for conf in r.boxes.conf.cpu().numpy().tolist()] if r.boxes.conf is not None else []
            box_list = r.boxes.xywh.cpu().numpy().tolist() if r.boxes.xywh is not None else []
            if not cls_list or not conf_list or not box_list:
                continue
            for cls, score, box in zip(cls_list, conf_list, box_list):
                if score > 0:
                    self.draw_cls = cls.upper()
                    self.draw_box = box
                    self.draw_frame = frame
                else:
                    self.draw_cls = None
                    self.draw_box = None
                self.frame_id += 1
                self.cls = cls
                if self.cls is not None and self.non_detect > self.NON_DETECT_THRESHOLD:
                    self.defect_id += 1
                    self.frame_end = self.frame_id
                    self.non_detect = 0
                    self.pre_cls = self.cls
                    self.cls = None
                    self.item_id = self.item_i_id
                if self.pre_cls != self.cls and self.cls is not None and self.pre_cls is not None:
                    self.defect_id += 1
                    self.frame_end = self.frame_id
                    self.cls = None
                    self.pre_cls = None
                    self.item_id = self.item_i_id
                def_dis = self.calculate_defect_location(self.frame_start, self.frame_end)
                self.non_detect = 0
                pred_list.append({
                    'shift': self.shift,
                    # 'frame': self.frame_id,
                    'sl_no': self.frame_id,
                    'item_id': self.item_id,
                    'track_id': self.defect_id,
                    'label': cls.upper(),
                    'score': score,
                    'x': box[0],
                    'y': box[1],
                    'w': box[2],
                    'h': box[3],
                    'defect_area': (box[2]* box[3]) * 0.3,
                    'item': self.item,
                    'category': self.category,
                    'date': self.current_date,
                    'time': datetime.datetime.now().strftime("%H%M%S"),
                    'defect_distance': def_dis
                })
    
        if not pred_list:
            self.draw_cls = None
            self.draw_box = None
            self.frame_id += 1
            pred_list.append({
                'shift': self.shift,
                # 'frame': self.frame_id,
                'sl_no': self.frame_id,
                'item_id': self.item_id,
                'track_id': '',
                'label': '',
                'score': 0,
                'x': 0,
                'y': 0,
                'w': 0,
                'h': 0,
                'defect_area' : 0,
                'item': self.item,
                'category': self.category,
                'date': self.current_date,
                'time': datetime.datetime.now().strftime("%H%M%S"),
                'defect_distance': 0})
            self.non_detect += 1
            self.cls = None
        self.draw_bounding_boxes_with_class(self.draw_frame,self.draw_box,self.draw_cls)
        if os.path.exists(self.csv_file_path):
            self.write_to_csv(pred_list, self.csv_file_path) 
        else:
            column_names = list(pred_list[0].keys())  # Extract keys from the first dictionary
            new_df = pd.DataFrame(pred_list[1:], columns=column_names)
            new_df.to_csv(self.csv_file_path, index=False)
   