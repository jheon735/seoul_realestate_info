import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
from geopy.geocoders import Nominatim
import time

gu_code = {11110 : "종로구", 11140 : "중구", 11170 : "용산구", 11200 : "성동구", 11215 : "광진구", 11230 : "동대문구",
        11260 : "중랑구", 11290 : "성북구", 11305 : "강북구", 11320 : "도봉구", 11350 : "노원구", 11380 : "은평구",
        11410 : "서대문구", 11440 : "마포구", 11470 : "양천구", 11500 : "강서구", 11530 : "구로구", 11545 : "금천구",
        11560 : "영등포구", 11590 : "동작구", 11620 : "관악구", 11650 : "서초구", 11680 : "강남구", 11710 : "송파구",
        11740 : "강동구"}

def data_download(url, key, year, month):
    """공공데이터포털 API를 활용헤 자료를 다운받아 pandas dataframe 형태로 변환해주는 함수

    Args:
        url (str): 요청할 url
        key (str): api key
        year (str): 4자리 년도
        month (str): 2자리 월 ex:03

    Returns:
        pandas dataframe : api 요청 결과
    """
    region = gu_code.keys()
    data = []
    for i in region:
        params ={'serviceKey' : f'{key}', 
                'LAWD_CD' : f'{i}', 
                'DEAL_YMD' : f'{year+month}'}
        response = requests.get(url, params=params).content
        soup = BeautifulSoup(response, 'lxml-xml')
        rows = soup.find_all('item')
        for row in rows:
            items = row.find_all()
            items_dict = {}
            items_dict["자치구"] = gu_code[i]
            for item in items:
                items_dict[item.name.strip()] = item.text.strip()
            data.append(items_dict)

    df = pd.DataFrame(data, columns=cols, dtype = object) 
    return df


def get_lat_lng(add):
    """ 주소를 입력하면 위경도를 출력

    Args:
        add (string): 지번 주소

    Returns:
        int : 위경도 값
    """
    geolocator = Nominatim(user_agent='South Korea')  # Fix the typo here
    location = geolocator.geocode(add)

    if location:
        latitude = location.latitude
        longitude = location.longitude
        return latitude, longitude
    else:
        print('지번 주소 문제')
        print(add)
        return None, None


def preprocessing_with_lat_lon(df, savefname, cols):
    """데이터 전처리를 위한 함수

    Args:
        df (pandas dataframe): 데이터 전처리를 할 dataframe
        savefname (str): 저장할 파일 이름
        cols (list): 최종 사용할 컬럼 이름
    """
    addrdf = df[~df.duplicated(subset=['자치구', '법정동', '지번'])]
    tt = time.time()
    newdata = []
    for idx, row in addrdf.iterrows():
        row['주소'] = "서울특별시 "+row["자치구"]+" "+row["법정동"] + " " + str(int(row["지번"]))
        lat, lon = get_lat_lng(row['주소'])
        row['위도'] = lat
        row['경도'] = lon
        if idx % 100 == 0:
            print("index_num:", idx, "소요시간:", int(time.time()-tt),"초")
        newdata.append(row)

    latlondf = pd.DataFrame(newdata, dtype = object) 

    findf = pd.merge(df, latlondf, on=['자치구', '법정동', '지번'], how='left', suffixes=('', '_drop'))
    findf = findf[cols].sort_values(by=cols[0])
    findf.to_csv(f"{savefname}", index=False)

def preprocessing(df, savefname, cols):
    """데이터 전처리를 위한 함수

    Args:
        df (pandas dataframe): 데이터 전처리를 할 dataframe
        savefname (str): 저장할 파일 이름
        cols (list): 최종 사용할 컬럼 이름
    """
    newdata = []
    df["주소"] = "서울특별시 " + df["자치구"].map(str) + " " + df["법정동"]

    df = pd.DataFrame(newdata, dtype = object) 
    df = df[cols].sort_values(by="계약일")
    df.to_csv(f"{savefname}", index=False)

# 아파트매매

cols = ['년','월', '일', '법정동', '아파트',  '전용면적', '거래금액', '건축년도',
        '지번', '지역코드', '층', '해제여부', '해제사유발생일', '거래유형', '중개사소재지', '등기일자', '매도자', '매수자','동']

# data_extract(url, key, dates, cols, "아파트매매", apartment_sell)

offtel_rent_url = "http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcOffiRent"
offtel_trade_url = "http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcOffiTrade"
house_trade_url = "	http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcSHTrade"
house_rent_url = "http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcSHRent"
apart_rent_url = "http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptRent"
apart_trade_url = 'http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade'
billa_rent_url = "http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcRHRent"
billa_trade_url = "http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcRHTrade"


region = [11110, 11140, 11170, 11200, 11215, 11230, 11260, 11290, 11305, 11320, 11350, 11380, 11410, 11440, 11470, 11500, 11530, 11545,
            11560, 11590, 11620 ,11650, 11680, 11710, 11740]

key = "ZU3VKtV/cyVYqylBKhohTTGbwd5/hq0d4YDqWyHz9kODNZMljBKxidxikPm6J4uY7MTEGHQfT4+FuK/UEGmNkQ=="

def data_download(url, key, year, month):
    data = []
    region = [11110]
    for i in region:
        params ={'serviceKey' : f'{key}', 
                'LAWD_CD' : f'{i}', 
                'DEAL_YMD' : f'{year+month}'}
        response = requests.get(url, params=params).content
        soup = BeautifulSoup(response, 'lxml-xml')
        rows = soup.find_all('item')
        for row in rows:
            items = row.find_all()
            items_dict = {}
            for item in items:
                items_dict[item.name.strip()] = item.text.strip()
            data.append(items_dict)
    df = pd.DataFrame(data) 
    print(df['지번'])

data_download(offtel_rent_url, key, '2024', '02')
apart_rent_cols = ['갱신요구권사용', '건축년도', '계약구분', '계약기간', '년', '단지', '법정동', '보증금', '시군구', '월',
                    '월세', '일', '전용면적', '종전계약보증금', '종전계약월세', '지번', '지역코드', '층']
billa_rent_cols = ['갱신요구권사용', '건축년도', '계약구분', '계약기간', '년', '법정동', '보증금액', '연립다세대', '월',
                    '월세금액', '일', '전용면적', '종전계약보증금', '종전계약월세', '지번', '지역코드', '층']
house_rent_cols = ['갱신요구권사용', '건축년도', '계약구분', '계약기간', '계약면적', '년', '법정동', '보증금액', '월',
                    '월세금액', '일', '종전계약보증금', '종전계약월세', '지역코드']
offtel_rent_cols = ['갱신요구권사용', '건축년도', '계약구분', '계약기간', '년', '단지', '법정동', '보증금', '시군구', '월',
                    '월세', '일', '전용면적', '종전계약보증금', '종전계약월세', '지번', '지역코드', '층']
apart_trade_cols = ['거래금액', '거래유형', '건축년도', '년', '단지', '매도자', '매수자', '법정동', '시군구', '월', '일',
                    '전용면적', '중개사소재지', '지번', '지역코드', '층', '해제사유발생일', '해제여부']
billa_trade_cols = ['거래금액', '거래유형', '건축년도', '년', '대지권면적', '등기일자', '매도자', '매수자', '법정동',
                    '연립다세대', '월', '일', '전용면적', '중개사소재지', '지번', '지역코드', '층', '해제사유발생일', '해제여부']
house_trade_cols = ['거래금액', '거래유형', '건축년도', '년', '대지면적', '매도자', '매수자', '법정동', '연면적', '월',
                    '일', '주택유형', '중개사소재지', '지번', '지역코드', '해제사유발생일', '해제여부']
offtel_trade_cols = ['거래금액', '거래유형', '건축년도', '년', '단지', '매도자', '매수자', '법정동', '시군구', '월', '일',
                    '전용면적', '중개사소재지', '지번', '지역코드', '층', '해제사유발생일', '해제여부']


tradecols = ['계약일', '자치구명', '법정동명', '주소', '건물명', '층', '위도', '경도', '물건금액(만원)',
            '건물면적(㎡)', '토지면적(㎡)', '권리구분', '취소일', '건축년도', '건물용도', '신고구분']
rentcols = ['계약일', '자치구명', '법정동명', '주소', '건물명', '층', '위도', '경도', '전월세 구분', '임대면적(㎡)', '보증금(만원)',
        '임대료(만원)', '계약기간', '신규갱신여부', '계약갱신권사용여부', '종전 보증금', '종전 임대료', '건축년도',
        '건물용도']

# df = pd.DataFrame(data)
# df["계약일"] = df["년"].map(str) + df["월"].map(str) + df["일"].map(str)
# df["주소"] = "서울특별시 "+ df[""] + " "

# cols =['계약일', '자치구명', '법정동명', '주소', '건물명', '층', '위도', '경도', '물건금액(만원)', '건물면적(㎡)', '토지면적(㎡)', 
#         '권리구분', '취소일', '건축년도', '건물용도', '신고구분']




# trade = pd.read_csv('data/seoul_realestate.csv', header = 0, skipinitialspace=True)
# trade = trade.drop(trade[trade['자치구코드'] > 20000].index)
# trade = trade.drop(['접수연도', '신고한 개업공인중개사 시군구명', '자치구코드', '법정동코드', '지번구분', '지번구분명'], axis=1)
# trade = trade.fillna(-9999)
# tt = time.time()
# newdata = []
# for idx, row in trade.iterrows():
#     if row['본번'] != -9999 and row['부번'] != 0:
#         row['주소'] = "서울특별시 "+row["자치구명"]+" "+row["법정동명"] + " " + str(int(row["본번"])) + "-" + str(int(row["부번"]))
#         lat, lon = get_lat_lng(row['주소'])
#         row['위도'] = lat
#         row['경도'] = lon
#     elif row['본번'] != -9999 and row['부번'] == 0:
#         row['주소'] = "서울특별시 "+row["자치구명"]+" "+row["법정동명"] + " " + str(int(row["본번"]))
#         lat, lon = get_lat_lng(row['주소'])
#         row['위도'] = lat
#         row['경도'] = lon
#     else:
#         row['주소'] = "서울특별시 "+row["자치구명"]+" "+row["법정동명"]
#         row['위도'] = -9999
#         row['경도'] = -9999
#     if idx == 10:
#         print(idx, time.time()-tt)
#         break
#     newdata.append(row)

# df = pd.DataFrame(newdata, dtype = object) 
# df = df.replace({-9999: None})
# df.to_csv(f'test.csv') 





# tradecols = ['계약일', '자치구명', '법정동명', '주소', '건물명', '층', '위도', '경도', '물건금액(만원)',
#             '건물면적(㎡)', '토지면적(㎡)', '권리구분', '취소일', '건축년도', '건물용도', '신고구분']

# rentcols = ['계약일', '자치구명', '법정동명', '주소', '건물명', '층', '위도', '경도', '전월세 구분', '임대면적(㎡)', '보증금(만원)',
#         '임대료(만원)', '계약기간', '신규갱신여부', '계약갱신권사용여부', '종전 보증금', '종전 임대료', '건축년도',
#         '건물용도']

# apikey = "28010AC2-7187-3642-BC8C-816A34DDF7CC"
# preprocessing('rent_test.csv', 'rent_test_result.csv', rentcols, apikey)
# preprocessing('trade_test.csv', 'trade_test_result.csv', tradecols, apikey)

# today = (datetime.now()+timedelta(hours=9)).strftime(r"%Y%m%d")
# querydate = today[:6]