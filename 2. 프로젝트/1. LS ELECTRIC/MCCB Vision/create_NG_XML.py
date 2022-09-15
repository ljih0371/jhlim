from azure.storage.blob import BlockBlobService, ContentSettings
import os, glob
import sys
from lxml import etree
import xml.etree.ElementTree as ET
import pandas as pd
import numpy as np
import cv2
import colorsys
from datetime import datetime, timedelta
from PIL import ImageFont, Image, ImageDraw
import time

# ======================================================================================================================
# ======================================================================================================================
## Blob Storage 계정정보 입력
container_name = 'mccb'
block_blob_service = BlockBlobService(account_name='cj01vision001', account_key='T7Q7tncoj0Er29cvCK6DsU9w0gnM0tvfYoWOc1Btacx+TySBxPdx+h+n3YUghgpRpZUcSfkHiGgLX98wGDXnmA==')

## 라인 리스트
## 추가 라인이 적용될 경우 하기 리스트에 라인 명을 추가해줘야함
## 라인명은 반드시 Blob의 라인 폴더 명과 일치해야함
## #2라인 2개 추가
line_list = ['ABH125c_1', 'ABH250c_1', 'ABH125c_2', 'ABH250c_2']
#line_list = ['ABH125c_1']

## D-1 날짜
## D-1 데이터를 배치로 가져오기 때문에 var1 는 일반적으로 1
## 변수에 따라 D-n 의 데이터를 가져올 수 있음
var1 = sys.argv[1]
day_var = float(var1)
yesterday = datetime.now() - timedelta(days=day_var)
check_dt = yesterday.strftime('%Y%m%d')
check_mon = check_dt[:-2]


# ======================================================================================================================
# ======================================================================================================================
## 각 NG 이미지에 해당하는 XML 파일을 맵핑하고, XML 파일이 존재하는 NG파일만 추출

def get_png_list(png_import_path, xml_import_path, f_condition, container_name, block_blob_service):

    # NG 이미지(png)
	for con_try in range(5):
		try:
			print("Try to get png list : " + str(con_try + 1))
			png_path_list = block_blob_service.list_blobs(container_name, png_import_path, timeout=1800)
		except:
			time.sleep(30)
			continue
		break

	png_path = [blob.name for blob in png_path_list if f_condition in blob.name]
	png_df = pd.DataFrame({'png_path': png_path})
	png_df['png_name'] = [x.split('/')[-1] for x in png_df['png_path']]

    # 라벨 정보(xml)
	for con_try in range(5):
		try:
			print("Try to get xml list: " + str(con_try + 1))
			xml_path_list = block_blob_service.list_blobs(container_name, xml_import_path, timeout=1800)
		except:
			time.sleep(30)
			continue
		break

	xml_path = [blob.name for blob in xml_path_list if f_condition in blob.name]
	xml_df = pd.DataFrame({'xml_path': xml_path})
	xml_df['xml_name'] = [x.split('/')[-1] for x in xml_df['xml_path']]
	xml_df['png_name'] = [x.replace('.xml', '.NG.png') for x in xml_df['xml_name']]

    # xml 파일이 있는 png만 추출
	png_df = pd.merge(png_df, xml_df, on='png_name', how='inner')

	return png_df

# ======================================================================================================================
# ======================================================================================================================
## 추출한 NG파일과 XML파일을 가지고 바운딩 이미지 생성
## 각 불량 유형별로 불량 바운딩 이미지 생성. 동일 이미지 내에서 해당하는 불량 유형만 하이라이트로 표시

def img_bounding(line, check_dt, df, rst_save_path, container_name, block_blob_service):

	png_path = df['png_path'].tolist()
	png_name = df['png_name'].tolist()
	xml_path = df['xml_path'].tolist()
	xml_name = df['xml_name'].tolist()

	clear_num = 0
    # png 파일 하나씩 처리
	for one_png_path, one_png_name, one_xml_path, one_xml_name in zip(png_path, png_name, xml_path, xml_name):

        # 기본 정의 -----------------------------------------------------------------------------------------
 		# 색, 선 두께, 폰트 크기 정의
		colors = [(95,253,239),  # yellow 
					(0,17,255)]    # red (하이라이트)
            
		thickness = 4
		fontScale = 1

        # XML -----------------------------------------------------------------------------------------------
        # 이미지 1장당 불량 영역이 1개 이상이므로 리스트 생성
		xml_defect_class, xml_xmin, xml_ymin, xml_xmax, xml_ymax = [], [], [], [], []

        # xml 파일 불러오기
		content_xml = block_blob_service.get_blob_to_text(container_name, one_xml_path).content

		tree = ET.ElementTree(ET.fromstring(content_xml))
		note = tree.getroot()

        # 파싱: 불량 유형
		for element in note.findall("object"):
			xml_defect_class.append(element.findtext("name"))

        # 파싱: 불량 영역 좌표
		for element in note.iter("bndbox"):
			xml_xmin.append(int(element.findtext("xmin")))
			xml_ymin.append(int(element.findtext("ymin")))
			xml_xmax.append(int(element.findtext("xmax")))
			xml_ymax.append(int(element.findtext("ymax")))
			

        # 결과 저장 -------------------------------------------------------------------------------------------
        # 이미지(1장)에 대해 n개 하이라이트 영역 그리기
		for i in range(len(xml_ymin)) : 
					
			# PNG -----------------------------------------------------------------------------------------------
			# 이미지 불러오기
			byte = block_blob_service.get_blob_to_bytes(container_name, one_png_path)
			byte2np = np.frombuffer(byte.content, dtype='uint8')
			image = cv2.imdecode(byte2np, cv2.IMREAD_COLOR)
			image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)

			# 그리기 --------------------------------------------------------------------------------------------
			# 이미지(1장)에 대한 불량영역(n개) 하나씩 처리
			for c, top, left, bottom, right in zip(xml_defect_class, xml_ymin, xml_xmin, xml_ymax, xml_xmax):

				try :

					# 좌표 지정
					top = max(0, np.floor(top + 0.5).astype('int32'))
					left = max(0, np.floor(left + 0.5).astype('int32'))
					bottom = min(image.shape[0], np.floor(bottom + 0.5).astype('int32'))  # y
					right = min(image.shape[1], np.floor(right + 0.5).astype('int32'))  # x

					# 불량영역 그리기
					cv2.rectangle(image, (left, top), (right, bottom), colors[0], thickness)

					# 불량유형 작성하기 (한글명 출력)  -- 나중에 필요할 시 사용
					#(text_width, text_height) = ImageFont.truetype("/NanumFont/NanumGothicBold.ttf", 50).getsize(class_name[c][0])
					#cv2.rectangle(image, (left, top), (left+text_width, top-text_height-5), colors[0], thickness=cv2.FILLED)
					#pill_image = Image.fromarray(image)
					#draw = ImageDraw.Draw(pill_image)
					#draw.text((left, top-text_height-5), class_name[c][0], font=ImageFont.truetype("/NanumFont/NanumGothicBold.ttf", 50), fill=(0,0,0))
					#image = cv2.cvtColor(cv2.cvtColor(np.array(pill_image), cv2.COLOR_RGB2BGR), cv2.COLOR_BGR2RGB)

				except:
					continue
						
				# 하이라이트 좌표 지정
				top_highlight = max(0, np.floor(xml_ymin[i] + 0.5).astype('int32'))
				left_highlight = max(0, np.floor(xml_xmin[i] + 0.5).astype('int32'))
				bottom_highlight = min(image.shape[0], np.floor(xml_ymax[i] + 0.5).astype('int32')) # y
				right_highlight = min(image.shape[1], np.floor(xml_xmax[i] + 0.5).astype('int32'))   # x
				
				# 하이라이트 (red)
				cv2.rectangle(image, (left_highlight, top_highlight), (right_highlight, bottom_highlight), colors[1], thickness)    
				
				# 하이라이트 부분 불량유형 작성하기 (한글명 출력)  -- 나중에 필요할 시 사용   
				#(text_width_highlight, text_height_highlight) = ImageFont.truetype("/NanumFont/NanumGothicBold.ttf", 50).getsize(class_name[xml_defect_class[i]][0])
				#cv2.rectangle(image, (left_highlight, top_highlight), (left_highlight+text_width_highlight, top_highlight-text_height_highlight-5), colors[1], thickness=cv2.FILLED)
				#pill_image = Image.fromarray(image)
				#draw = ImageDraw.Draw(pill_image)
				#draw.text((left_highlight, top_highlight-text_height_highlight-5), class_name[xml_defect_class[i]][0], font=ImageFont.truetype("/NanumFont/NanumGothicBold.ttf", 50), fill=(0,0,0))
				#image = cv2.cvtColor(cv2.cvtColor(np.array(pill_image), cv2.COLOR_RGB2BGR), cv2.COLOR_BGR2RGB)

			# 결과 저장 -------------------------------------------------------------------------------------------
			
			deft_type = str(xml_defect_class[i])
			box_xy = str(xml_xmin[i]) + '_' + str(xml_ymin[i]) + '_' + str(xml_xmax[i]) + '_' + str(xml_ymax[i]) 
			side = one_png_name.split('.')[2]
            
			rst_name = rst_save_path + '/' + side + '/' + one_png_name[:one_png_name.find('(')] + '.' + side + '.' + deft_type + '.' + box_xy + '.jpg'
            # save_path/20201106_181839.P010100B0118B4DX5V01_00001350008.LINE_TAP.DL_MDEL_HAND_LOC_DEFT.2305_807_2338_318.jpg
            
			_, img_rst = cv2.imencode('.jpg', image)
			bytes_rst = img_rst.tobytes()

			block_blob_service.create_blob_from_bytes(container_name,
                    		                        rst_name,
                            		                bytes_rst,
                                    		        content_settings=ContentSettings(content_type='image/jpg'))

			clear_num += 1

	print(datetime.now())
	print('전체(NG) : ' + str(len(png_path)) + '개\n'
          '처리 완료(NG_XML) : ' + str(clear_num) + '개\n')
# ======================================================================================================================
# ======================================================================================================================
## 각 라인 별로 수행

for line in line_list:
	print('\n>> '+ check_dt +' '+ line + ' : 시작')
    # PNG 이미지 & XML 파일 경로 지정
	png_import_path = line + '/NG/' + check_mon
	xml_import_path = line + '/XML/' + check_mon

	print(line+ ' : PNG 이미지 & XML 파일 경로 지정 완료')

    # 결과 JPG 이미지 저장 경로 지정
	rst_save_path = line + '/NG_XML/' + check_mon
	f_condition = '/' + check_dt + '_'

	print(line+ ' : 결과 JPG 이미지 저장 경로 지정 완료')
	
	try:
	# 지정 경로의 파일 목록 가져오기
		png_df = get_png_list(png_import_path, xml_import_path, f_condition, container_name, block_blob_service)

		if not png_df.empty :
	# 처리
			img_bounding(line, check_dt, png_df, rst_save_path, container_name, block_blob_service)
		else : 
			print(line + ' png_df is empty')

	except Exception as e:
		print(line+' exception : '+ str(e))
		continue



