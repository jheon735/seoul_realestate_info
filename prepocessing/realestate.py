import pandas as pd
import time
import requests

def preprocessing(csvfname, savefname, cols, apikey):
    """데이터 전처리를 위한 함수

    위경도 변환을 vworld의 api를 사용하여 시간을 단축하였다
    다만 vwolrd api의 일 요청건수 제한이 40,000건이므로 40,000건 이상으 자료 변환시 확인이 필요하다

    Args:
        csvfname (str): 데이터 전처리를 할 원본 csv파일 이름
        savefname (str): 저장할 파일 이름
        cols (list): 최종 사용할 컬럼 이름
        apikey (str): 위경도 변환을 위한 vworld api 키 값
    """
    df = pd.read_csv(csvfname, header = 0, skipinitialspace=True)
    df = df.fillna(-9999)
    addrdf = df[~df.duplicated(subset=['법정동명', '본번', '부번'])]
    tt = time.time()
    apiurl = "https://api.vworld.kr/req/address?"
    newdata = []
    count = 0
    for idx, row in addrdf.iterrows():
        if row['본번'] != -9999 and row['부번'] != 0:
            row['주소'] = "서울특별시 "+row["자치구명"]+" "+row["법정동명"] + " " + str(int(row["본번"])) + "-" + str(int(row["부번"]))
            params = {
                "service": "address",
                "request": "getcoord",
                "crs": "epsg:4326",
                f"address": f"{row['주소']}",
                "format": "json",
                "type": "parcel",
                "key": f"{apikey}"
            }
            try:
                response = requests.get(apiurl, params=params)
            except Exception as e:
                print(e)
                break
            count += 1
            if response.status_code == 200:
                try:
                    lon = response.json()["response"]["result"]['point']['x']
                    lat = response.json()["response"]["result"]['point']['y']
                    row['위도'] = lat
                    row['경도'] = lon
                except KeyError:
                    print(response.json())
                    row['위도'] = -9999
                    row['경도'] = -9999
            else:
                row['위도'] = -9999
                row['경도'] = -9999
        elif row['본번'] != -9999 and row['부번'] == 0:
            row['주소'] = "서울특별시 "+row["자치구명"]+" "+row["법정동명"] + " " + str(int(row["본번"]))
            params = {
                "service": "address",
                "request": "getcoord",
                "crs": "epsg:4326",
                f"address": f"{row['주소']}",
                "format": "json",
                "type": "parcel",
                "key": f"{apikey}"
            }
            try:
                response = requests.get(apiurl, params=params)
            except Exception as e:
                print(e)
                break
            count += 1
            if response.status_code == 200:
                try:
                    lon = response.json()["response"]["result"]['point']['x']
                    lat = response.json()["response"]["result"]['point']['y']
                    row['위도'] = lat
                    row['경도'] = lon
                except KeyError:
                    print(response.json())
                    row['위도'] = -9999
                    row['경도'] = -9999
            else:
                row['위도'] = -9999
                row['경도'] = -9999
        else:
            row['주소'] = "서울특별시 "+row["자치구명"]+" "+row["법정동명"]
            row['위도'] = -9999
            row['경도'] = -9999
        if idx % 100 == 0:
            print("index_num:", idx, "소요시간:", int(time.time()-tt),"초")
            print("API 호출 횟수:", count)
        newdata.append(row)

    latlondf = pd.DataFrame(newdata, dtype = object) 

    findf = pd.merge(df, latlondf, on=['자치구명', '법정동명', '본번', '부번'], how='left', suffixes=('', '_drop'))
    findf = findf.drop(findf.filter(regex='_drop').columns, axis=1)
    findf = findf[cols].sort_values(by="계약일")
    findf = findf.replace({-9999: None})
    findf.to_csv(f"{savefname}", index=False)

tradecols = ['계약일', '자치구명', '법정동명', '주소', '건물명', '층', '위도', '경도', '물건금액(만원)',
            '건물면적(㎡)', '토지면적(㎡)', '권리구분', '취소일', '건축년도', '건물용도', '신고구분']

rentcols = ['계약일', '자치구명', '법정동명', '주소', '건물명', '층', '위도', '경도', '전월세 구분', '임대면적(㎡)', '보증금(만원)',
        '임대료(만원)', '계약기간', '신규갱신여부', '계약갱신권사용여부', '종전 보증금', '종전 임대료', '건축년도',
        '건물용도']

preprocessing('rent_test.csv', 'rent_test_result.csv', rentcols, apikey)
preprocessing('trade_test.csv', 'trade_test_result.csv', tradecols, apikey)
