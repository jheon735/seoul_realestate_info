import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
from geopy.geocoders import Nominatim
import time
import numpy as np

gu_code = {11110 : "종로구", 11140 : "중구", 11170 : "용산구", 11200 : "성동구", 11215 : "광진구", 11230 : "동대문구",
        11260 : "중랑구", 11290 : "성북구", 11305 : "강북구", 11320 : "도봉구", 11350 : "노원구", 11380 : "은평구",
        11410 : "서대문구", 11440 : "마포구", 11470 : "양천구", 11500 : "강서구", 11530 : "구로구", 11545 : "금천구",
        11560 : "영등포구", 11590 : "동작구", 11620 : "관악구", 11650 : "서초구", 11680 : "강남구", 11710 : "송파구",
        11740 : "강동구"}

def data_download(url, key, year, month, type):
    """공공데이터포털 API를 활용헤 자료를 다운받아 pandas dataframe 형태로 변환해주는 함수

    Args:
        url (str): 요청할 url
        key (str): api key
        year (str): 4자리 년도
        month (str): 2자리 월 ex:03
        type (str) : 건물용도

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
            items_dict['자치구명'] = gu_code[i]
            items_dict['건물용도'] = type
            for item in items:
                items_dict[item.name.strip()] = item.text.strip()
            data.append(items_dict)

    df = pd.DataFrame(data, dtype = object) 
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


def rent_preprocessing_with_lat_lon(df, col_change_dict, fincols):
    """ 전월세 데이터 전처리를 위한 함수

    Args:
        df (pandas dataframe): 데이터 전처리를 할 dataframe
        savefname (str): 저장할 파일 이름
        cols (list): 최종 사용할 컬럼 이름
    """
    df["계약일"] = df["년"].map(str) + df["월"].map(str) + df["일"].map(str)
    df = df.rename(columns=col_change_dict)
    addrdf = df[~df.duplicated(subset=['자치구명', '법정동명', '지번'])]
    tt = time.time()
    newdata = []
    for idx, row in addrdf.iterrows():
        if row['계약갱신권사용여부'] == '사용':
            row['계약갱신권사용여부'] = '○'
        else:
            row['계약갱신권사용여부'] = None
        if row['임대료(만원)'] == 0 or row['임대료(만원)'] == '0':
            row['전월세 구분'] = '전세'
        else:
            row['전월세 구분'] = '월세'
        row['주소'] = "서울특별시 "+row["자치구명"]+" "+row["법정동명"] + " " + row["지번"]
        lat, lon = get_lat_lng(row['주소'])
        row['위도'] = lat
        row['경도'] = lon
        if idx % 100 == 0:
            print("index_num:", idx, "소요시간:", int(time.time()-tt),"초")
        newdata.append(row)

    latlondf = pd.DataFrame(newdata, dtype = object) 

    findf = pd.merge(df, latlondf, on=['자치구명', '법정동명', '지번'], how='left', suffixes=('', '_drop'))
    findf = findf[fincols].sort_values(by=fincols[0])
    return findf

def rent_preprocessing(df, col_change_dict, fincols):
    """데이터 전처리를 위한 함수

    Args:
        df (pandas dataframe): 데이터 전처리를 할 dataframe
        savefname (str): 저장할 파일 이름
        cols (list): 최종 사용할 컬럼 이름
    """
    df["계약일"] = df["년"].map(str) + df["월"].map(str) + df["일"].map(str)
    df = df.rename(columns=col_change_dict)
    df["주소"] = "서울특별시 " + df["자치구명"].map(str) + " " + df["법정동명"] 
    if "건축년도" not in df.columns:
        df["건축년도"] = None
    df['전월세 구분'] = np.where((df["임대료(만원)"].values == 0) | (df['임대료(만원)'].values == '0'), "전세","월세" )
    df["위도"] = None
    df["경도"] = None
    df["건물명"] = None
    df["층"] = None

    df = df[fincols].sort_values(by=fincols[0])
    return df

def trade_preprocessing_with_lat_lon(df, col_change_dict, fincols):
    """ 매매 데이터 전처리를 위한 함수

    Args:
        df (pandas dataframe): 데이터 전처리를 할 dataframe
        savefname (str): 저장할 파일 이름
        cols (list): 최종 사용할 컬럼 이름
    """
    df["계약일"] = df["년"].map(str) + df["월"].map(str) + df["일"].map(str)
    df = df.rename(columns=col_change_dict)
    addrdf = df[~df.duplicated(subset=['자치구명', '법정동명', '지번'])]
    tt = time.time()
    newdata = []
    for idx, row in addrdf.iterrows():
        if '토지면적(㎡)' not in row.index:
            row['토지면적(㎡)'] = None
        if '건물명' not in row.index:
            row['건물명'] = None
        if '층' not in row.index:
            row['층'] = None
        row['권리구분'] = None
        row['주소'] = "서울특별시 "+row["자치구명"]+" "+row["법정동명"] + " " + row["지번"]
        lat, lon = get_lat_lng(row['주소'])
        row['위도'] = lat
        row['경도'] = lon
        if idx % 100 == 0:
            print("index_num:", idx, "소요시간:", int(time.time()-tt),"초")
        newdata.append(row)

    latlondf = pd.DataFrame(newdata, dtype = object) 

    findf = pd.merge(df, latlondf, on=['자치구명', '법정동명', '지번'], how='left', suffixes=('', '_drop'))
    findf = findf[fincols].sort_values(by=fincols[0])
    return findf


def trade_preprocessing(df, col_change_dict, fincols):
    """데이터 전처리를 위한 함수

    Args:
        df (pandas dataframe): 데이터 전처리를 할 dataframe
        savefname (str): 저장할 파일 이름
        cols (list): 최종 사용할 컬럼 이름
    """
    df["계약일"] = df["년"].map(str) + df["월"].map(str) + df["일"].map(str)
    df = df.rename(columns=col_change_dict)
    df["주소"] = "서울특별시 " + df["자치구명"].map(str) + " " + df["법정동명"] + " " + df["지번"]
    df["위도"] = None
    df["경도"] = None
    df["건물명"] = None
    df["층"] = None
    df["권리구분"] = None
    df = df[fincols].sort_values(by=fincols[0])
    return df

def dfsumnsave(dfs, type, year, month):
    savedf = pd.concat(dfs)
    savedf.to_csv(f'{type}_{year}{month}.csv', index=False)


def main(key, year, month)
    offtel_rent_url = "http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcOffiRent"
    offtel_trade_url = "http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcOffiTrade"
    house_trade_url = "	http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcSHTrade"
    house_rent_url = "http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcSHRent"
    apart_rent_url = "http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptRent"
    apart_trade_url = 'http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade'
    billa_rent_url = "http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcRHRent"
    billa_trade_url = "http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcRHTrade"

    apart_rent_change = {'갱신요구권사용':'계약갱신권사용여부', '계약구분':'신규갱신여부', '아파트':'건물명', '법정동':'법정동명', '보증금액':'보증금(만원)',
                        '시군구':'자치구명', '월세금액':'임대료(만원)', '전용면적':'임대면적(㎡)', '종전계약보증금':'종전 보증금', '종전계약월세':'종전 임대료' }
    billa_rent_change = {'갱신요구권사용':'계약갱신권사용여부', '계약구분':'신규갱신여부', '연립다세대':'건물명', '법정동':'법정동명', '보증금액':'보증금(만원)',
                        '월세금액':'임대료(만원)', '전용면적':'임대면적(㎡)', '종전계약보증금':'종전 보증금', '종전계약월세':'종전 임대료' }
    house_rent_change = {'갱신요구권사용':'계약갱신권사용여부', '계약구분':'신규갱신여부', '법정동':'법정동명', '보증금액':'보증금(만원)',
                        '월세금액':'임대료(만원)', '계약면적':'임대면적(㎡)', '종전계약보증금':'종전 보증금', '종전계약월세':'종전 임대료' }
    offtel_rent_change = {'갱신요구권사용':'계약갱신권사용여부', '계약구분':'신규갱신여부', '단지':'건물명', '법정동':'법정동명', '보증금':'보증금(만원)',
                        '월세':'임대료(만원)', '전용면적':'임대면적(㎡)', '종전계약보증금':'종전 보증금', '종전계약월세':'종전 임대료' }
    apart_trade_change = {'거래금액':'물건금액(만원)', '거래유형':'신고구분', '아파트':'건물명', '법정동':'법정동명',
                        '전용면적':'건물면적(㎡)', '해제사유발생일':'취소일'}
    billa_trade_change = {'거래금액':'물건금액(만원)', '거래유형':'신고구분', '연립다세대':'건물명', '법정동':'법정동명',
                        '전용면적':'건물면적(㎡)', '대지권면적':'토지면적(㎡)', '해제사유발생일':'취소일'}
    house_trade_change = {'거래금액':'물건금액(만원)', '거래유형':'신고구분', '법정동':'법정동명', 
                        '연면적':'건물면적(㎡)', '대지면적':'토지면적(㎡)', '해제사유발생일':'취소일'}
    offtel_trade_change = {'거래금액':'물건금액(만원)', '거래유형':'신고구분', '단지':'건물명', '법정동':'법정동명',
                        '전용면적':'건물면적(㎡)', '해제사유발생일':'취소일'}

    tradecols = ['계약일', '자치구명', '법정동명', '주소', '건물명', '층', '위도', '경도', '물건금액(만원)',
                '건물면적(㎡)', '토지면적(㎡)', '권리구분', '취소일', '건축년도', '건물용도', '신고구분']
    rentcols = ['계약일', '자치구명', '법정동명', '주소', '건물명', '층', '위도', '경도', '전월세 구분', '임대면적(㎡)', '보증금(만원)',
            '임대료(만원)', '계약기간', '신규갱신여부', '계약갱신권사용여부', '종전 보증금', '종전 임대료', '건축년도',
            '건물용도']

    apartrent = data_download(apart_rent_url, key, year, month, '아파트')
    billarrent = data_download(billa_rent_url, key, year, month, '연립다세대')
    houserent = data_download(house_rent_url, key, year, month, '단독다가구')
    offrent = data_download(offtel_rent_url, key, year, month, '오피스텔')
    apartrent = rent_preprocessing_with_lat_lon(apartrent.head(10), apart_rent_change, rentcols)
    billarrent = rent_preprocessing_with_lat_lon(billarrent.head(10), billa_rent_change, rentcols)
    houserent = rent_preprocessing(houserent, house_rent_change, rentcols)
    offrent = rent_preprocessing_with_lat_lon(offrent.head(10), offtel_rent_change, rentcols)

    dfsumnsave([apartrent, billarrent, houserent, offrent], 'rent', year, month)

    aparttrade = data_download(apart_trade_url, key, year, month, '아파트')
    billartrade = data_download(billa_trade_url, key, year, month, '연립다세대')
    housetrade = data_download(house_trade_url, key, year, month, '단독다가구')
    offtrade = data_download(offtel_trade_url, key, year, month, '오피스텔')
    aparttrade = trade_preprocessing_with_lat_lon(aparttrade.head(10), apart_trade_change, tradecols)
    billartrade = trade_preprocessing_with_lat_lon(billartrade.head(10), billa_trade_change, tradecols)
    housetrade = trade_preprocessing(housetrade, house_trade_change, tradecols)
    offtrade = trade_preprocessing_with_lat_lon(offtrade.head(10), offtel_trade_change, tradecols)

    dfsumnsave([aparttrade, billartrade, housetrade, offtrade], 'trade', year, month)

main(key, '2024', '05')


"""
# API column info
apart_rent_cols = ['갱신요구권사용', '건축년도', '계약구분', '계약기간', '년', '아파트', '법정동', '보증금액', '시군구', '월',
                    '월세금액', '일', '전용면적', '종전계약보증금', '종전계약월세', '지번', '지역코드', '층']
billa_rent_cols = ['갱신요구권사용', '건축년도', '계약구분', '계약기간', '년', '법정동', '보증금액', '연립다세대', '월',
                    '월세금액', '일', '전용면적', '종전계약보증금', '종전계약월세', '지번', '지역코드', '층']
house_rent_cols = ['갱신요구권사용', '건축년도', '계약구분', '계약기간', '계약면적', '년', '법정동', '보증금액', '월',
                    '월세금액', '일', '종전계약보증금', '종전계약월세', '지역코드']
offtel_rent_cols = ['갱신요구권사용', '건축년도', '계약구분', '계약기간', '년', '단지', '법정동', '보증금', '시군구', '월',
                    '월세', '일', '전용면적', '종전계약보증금', '종전계약월세', '지번', '지역코드', '층']
apart_trade_cols = ['거래금액', '거래유형', '건축년도', '년', '아파트', '매도자', '매수자', '법정동', '시군구', '월', '일',
                    '전용면적', '중개사소재지', '지번', '지역코드', '층', '해제사유발생일', '해제여부']
billa_trade_cols = ['거래금액', '거래유형', '건축년도', '년', '대지권면적', '등기일자', '매도자', '매수자', '법정동',
                    '연립다세대', '월', '일', '전용면적', '중개사소재지', '지번', '지역코드', '층', '해제사유발생일', '해제여부']
house_trade_cols = ['거래금액', '거래유형', '건축년도', '년', '대지면적', '매도자', '매수자', '법정동', '연면적', '월',
                    '일', '주택유형', '중개사소재지', '지번', '지역코드', '해제사유발생일', '해제여부']
offtel_trade_cols = ['거래금액', '거래유형', '건축년도', '년', '단지', '매도자', '매수자', '법정동', '시군구', '월', '일',
                    '전용면적', '중개사소재지', '지번', '지역코드', '층', '해제사유발생일', '해제여부']
"""