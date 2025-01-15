# 🌾CAMP (Climate And Market Prediction)
## 프로젝트 개요
### **1.1. 주제 선정 배경**

- 최근 배추 가격 상승의 주요 원인이 기후 변화로 인한 작황 부진이라는 기사를 읽게 됨
    → 기후 변화가 농산물 가격에 미치는 영향 분석의 필요성을 체감
    

### **1.2. 프로젝트 내용**

- 기후 데이터와 도매·소매 가격 데이터 활용
- 정적 자원뿐만 아니라 동적 데이터를 효과적으로 수집하고 처리하기 위해 ETL 과정을 자동화
- 머신러닝 기술을 활용해 가격 변동의 원인을 파악하고 미래 가격을 예측하는 모델 구축
- 모니터링 시스템을 통해 분산된 서버 상태를 실시간으로 확인하며 데이터 흐름의 안정성을 유지

<aside>


## 📍 프로젝트 시각화 결과
### 품목(중분류) 거래 규모
<table>
  <tr>
    <td align="center">
      <img src="https://github.com/ClimateAndMarketPrediction/camp/blob/develop/images/visualization_0.png" style="height: 500px;">
    </td>
    <td align="center">
      <img src="https://github.com/ClimateAndMarketPrediction/camp/blob/develop/images/visualization_1.png" style="height: 500px;">
    </td>
  </tr>
  <tr>
    <td align="center">
      <span>도매시장 별 거래 규모</span>
    </td>
    <td align="center">
      <span>원산지별 품목 거래 규모</span>
    </td>
  </tr>
</table>



### 품목 별 가격 정보
<table>
  <tr>
    <td align="center">
      <img src="https://github.com/ClimateAndMarketPrediction/camp/blob/develop/images/visualization_2.png">
    </td>
  </tr>
  <tr>
    <td align="center">
      <span> 월별 & 최근 일주일 품목별 가격 정보 </span>
    </td>
  </tr>
</table>

### 반입 현황
<table>
  <tr>
    <td align="center">
      <img src="https://github.com/ClimateAndMarketPrediction/camp/blob/develop/images/visualization_3.png">
    </td>
  </tr>
  <tr>
    <td align="center">
      <span>품목별 반입 현황 & 지역별 반입 현황</span>
    </td>
  </tr>
</table>


### 기후 & 도매 상관관계

<table>
  <tr>
    <td align="center">
      <img src="https://github.com/ClimateAndMarketPrediction/camp/blob/develop/images/visualization_4.png"/>
    </td>
    <td align="center">
      <img src="https://github.com/ClimateAndMarketPrediction/camp/blob/develop/images/visualization_6.png" />
    </td>
  </tr>
  <tr>
    <td align="center">
      <span>월별 상위 5개 품목의 도매 물량 및 재배 시작 시기 평균 온도</span>
    </td>
    <td align="center">
      <span>상위 20개 재배지역 월별 강수량</span>
    </td>
  </tr>
</table>


### 도매 & 소매 상관관계
<table>
  <tr>
    <td>
      <img src="https://github.com/ClimateAndMarketPrediction/camp/blob/develop/images/visualization_5.png" />
  </tr>
  <tr>
    <td align="center">
      <span>도매가 소매가 비교 & 1년 전과 현재의 소매가 비교</span>
    </td>
  </tr>
</table>

</br>

## DAGS & GLUE JOBS
<table>
  <tr>
      <td align="center">
          <img src="https://github.com/ClimateAndMarketPrediction/camp/blob/develop/images/dags.png">
      </td>
      <td align="center">
          <img src="https://github.com/ClimateAndMarketPrediction/camp/blob/develop/images/glue_jobs.png">
      </td>
  </tr>
  <tr>
    <td align="center">
      <span>Airflow DAGS</span>
    </td>
    <td align="center">
      <span>AWS GLUE JOBS</span>
    </td>
  </tr>
</table>

### DAG FLOW
<table>
  <tr>
      <td align="center">
          <img src="https://github.com/ClimateAndMarketPrediction/camp/blob/develop/images/dagflow_1.png">
      </td>
      <td align="center">
        <img src="https://github.com/ClimateAndMarketPrediction/camp/blob/develop/images/dagflow_2.png">
      </td>
  </tr>
  <tr>
    <td align="center">
      <span>Static data DAGS</span>
    </td>
    <td align="center">
      <span>Dynamic data DAGS</span>
    </td>
  </tr>
  
</table>

</br>

## ML

<span>- **사용 모델**: LightGBM</span></br>
<span>- **사용 데이터셋**: 날씨 데이터와 도매 가격 데이터를 Join하여 생성한 약 1.5GB의 training set</span></br>
<span>- **feature & label**: 도매로 판매되기 전 6개월간의 월간 날씨 데이터와 도매 당시 물량 등을 학습하여 ‘사과’1개 / 전체 품목 에한 kg당 가격 예측</span>
<table>
  <tr>
    <td align="center">
        <img src="https://github.com/ClimateAndMarketPrediction/camp/blob/develop/images/ml_1.png">
    </td>
    <td align="center">
        <img src="https://github.com/ClimateAndMarketPrediction/camp/blob/develop/images/ml_2.png">
    </td>
  </tr>
  <tr>
    <td align="center">
      <span>하나의 중분류에 대한 가격 예측 정확도 <br> Explained Variance Score: 약 0.91</span>
    </td>
    <td align="center">
      <span>전체 품목에 대한 가격 예측 정확도 <br> Explained Variance Score: 약 0.74</span>
    </td>
  </tr>
</table>

</br>

## 💾 테이블 명세
<table>
  <tr>
    <td align="center">
        <img src="https://github.com/ClimateAndMarketPrediction/camp/blob/develop/images/raw_data.png">
    </td>
    <td align="center">
        <img src="https://github.com/ClimateAndMarketPrediction/camp/blob/develop/images/analytics.png">
    </td>
    <td align="center">
        <img src="https://github.com/ClimateAndMarketPrediction/camp/blob/develop/images/data.png">
    </td>
  </tr>
  <tr>
    <td align="center">
      <b>raw_data</b>
    </td>
    <td align="center">
      <b>analytics_data</b>
    </td>
    <td align="center">
      <span>ETL& ELT 과정을 거친 후 데이터 크기</span>
    </td>
  </tr>
</table>

</br>

## ⚙System Architecture

### 시스템 아키텍처

<table>
  <td align="center">
    <img src="https://github.com/ClimateAndMarketPrediction/camp/blob/develop/images/architecture.png">
  </td>
</table>

###  🛠 기술 스택 및 활용
<table align="center">
    <tr>
        <th align="center">아키텍처</th>
        <th align="center">기술 활용</th>
    </tr>
    <tr>
        <td valign="top">
            <h3 align="center">Infra</h3>
            <p align="center">
                <img src="https://img.shields.io/badge/ubuntu-E95420?&logo=ubuntu&logoColor=white">
                <img src="https://img.shields.io/badge/amazon EC2-FF9900?&logo=amazon ec2&logoColor=white">
                <img src="https://img.shields.io/badge/amazon RDS-527FFF?&logo=amazonrds&logoColor=white">
                <img src="https://img.shields.io/badge/amazon Elasticache-C925D1?&logo=amazonelasticache&logoColor=white">
            </p>
            <h3 align="center">ETL & ELT</h3>
            <p align="center">
                <img src="https://img.shields.io/badge/Apache airflow-017CEE?&logo=apacheairflow&logoColor=white">
                <img src="https://img.shields.io/badge/Apache%20Spark-E25A1C?&logo=apachespark&logoColor=white">
                <img src="https://img.shields.io/badge/glue-7248B9?&logo=amazonglue&logoColor=white">
                </br>
                <img src="https://img.shields.io/badge/amazon Redshift-8C4FFF?&logo=amazonredshift&logoColor=white">
                <img src="https://img.shields.io/badge/amazon S3-569A31?&logo=amazons3&logoColor=white">
                <img src="https://img.shields.io/badge/Python-3776AB?&logo=python&logoColor=white">
                <img src="https://img.shields.io/badge/pandas-150458?&logo=pandas&logoColor=white">
            </p>
            <h3 align="center">Monitoring</h3>
            <p align="center">
                <img src="https://img.shields.io/badge/prometheus-FF4000?&logo=prometheus&logoColor=white">
                <img src="https://img.shields.io/badge/grafana-FF8000?&logo=grafana&logoColor=white">
            </p>
            <h3 align="center">Visualization</h3>
            <p align="center">
                <img src="https://img.shields.io/badge/preset-00B992?logoColor=white" alt="preset" />
            </p>
            <h3 align="center">Co-work tool</h3>
            <p align="center">
                <img src="https://img.shields.io/badge/github-181717?&logo=github&logoColor=white">
                <img src="https://img.shields.io/badge/Notion-232F3E?&logo=Notion&logoColor=white">
                <img src="https://img.shields.io/badge/slack-E4637C?&logo=slack&logoColor=white">
            </p>
        </td>
        <td valign="center">
            <table align="center">
                <tr>
                    <th>활용 기술</th>
                    <th>비고</th>
                </tr>
                <tr>
                    <td>EC2</td>
                    <td>Airflow 서버 및 모니터링 서버</td>
                </tr>
                <tr>
                    <td>RDS</td>
                    <td>Airflow Metadata DB</td>
                </tr>
                <tr>
                    <td>Elasticache</td>
                    <td>Airflow Redis</td>
                </tr>
                <tr>
                    <td>Airflow</td>
                    <td>EL 워크플로우 및 스케줄링</td>
                </tr>
                <tr>
                    <td>Glue</td>
                    <td>서버리스 Spark 환경 제공</td>
                </tr>
                <tr>
                    <td>Spark</td>
                    <td>ELT 워크플로우 및 데이터 처리</td>
                </tr>
                <tr>
                    <td>S3</td>
                    <td>Data Lake 및 임시 저장소</td>
                </tr>
                <tr>
                    <td>Redshift</td>
                    <td>Data Warehouse</td>
                </tr>
                <tr>
                    <td>Preset</td>
                    <td>대시보드 및 시각화 도구</td>
                </tr>
                <tr>
                    <td>Prometheus</td>
                    <td>시스템 메트릭 수집</td>
                </tr>
                <tr>
                    <td>Grafana</td>
                    <td>시스템 메트릭 실시간 대시보드 및 시각화</td>
                </tr>
            </table>
        </td>
    </tr>
</table>

</br>
</br>



## 😊멤버
<table>
  <tr>
    <td align="center">
      <a href="https://github.com/yygs321">
        <img src="https://github.com/yygs321.png" alt="박소민" />
      </a>
    </td>
    <td align="center">
      <a href="https://github.com/hosic2">
        <img src="https://github.com/hosic2.png" alt="김주호" />
      </a>
    </td>
     <td align="center">
      <a href="https://github.com/bkshin01">
        <img src="https://github.com/bkshin01.png" alt="신보경" />
      </a>
    </td>
    <td align="center">
      <a href="https://github.com/Mollis-Kim">
        <img src="https://github.com/Mollis-Kim.png" alt="김선재" />
      </a>
    </td>
  </tr>
  <tr>
    <td align="center">
      <a href="https://github.com/yygs321">
        <b>박소민</b>
      </a>
    </td>
    <td align="center">
      <a href="https://github.com/zoobeancurd">
        <b>김주호</b>
      </a>
    </td>
     <td align="center">
      <a href="https://github.com/Nyhazzy">
        <b>신보경</b>
      </a>
    </td>
    <td align="center">
      <a href="https://github.com/Mollis-Kimy">
        <b>김선재</b>
      </a>
    </td>
  </tr>
  <tr>
    <td align="center">
      <span>날씨 데이터 파이프라인 구축<br>인프라 & 시각화<br>모니터링</span>
    </td>
    <td align="center">
      <span>소매 데이터 파이프라인 구축<br>인프라 & 시각화<br>모니터링</span>
    </td>
    <td align="center">
      <span>도매 동적 데이터 파이프라인 구축<br>인프라 & 시각화<br>ML</span>
    </td>
    <td align="center">
      <span>도매 정적 데이터 파이프라인 구축<br>인프라 & 시각화<br>ML</span>
    </td>
  </tr>
</table>
