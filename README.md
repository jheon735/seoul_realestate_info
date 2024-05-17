# seoul_realestate_info
데이터 전처리를 위한 프로그램

- realestate.py
  - 서울 열린 데이터 광장에서 제공하는 주택 매매, 전월세 csv파일에서 필요한 컬럼만 추출하는 전처리 프로그램
  - http://data.seoul.go.kr/dataList/OA-21275/S/1/datasetView.do 페이지에서 매매가 csv파일을 받는다
  - http://data.seoul.go.kr/dataList/OA-21276/S/1/datasetView.do 페이지에서 전월세 csv파일을 받는다
  - https://www.vworld.kr/dev/v4dv_geocoderguide2_s001.do에서 API 권한을 얻어 key값을 얻는다
  - preprocessing 함수를 사용하여 데이터 전처리를 진행한다
    - csvfname : 위에서 다운로드 한 전처리 할 csv파일의 경로를 포함한 파일 이름
    - savefname : 전처리 완료한 파일을 저장할 파일이름
    - cols : 전처리 후 저장할 컬럼 목록. 매매의 경우 tradecols, 전월세의 경우 rentcols 사용
    - apikey : vworld의 key 값
  
- update_data.py
  - 공공데이터포털 국토교통부의 주택형태별 매매, 전월세 api를 활용하여 매달 새로운 정보를 업데이트 할 수 있도록 전처리 해주는 프로그램
  - 
