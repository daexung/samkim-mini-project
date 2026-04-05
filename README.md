# AWS Reservation Analytics & EKS Infrastructure

예약 데이터 분석을 위한 서버리스 파이프라인 및 고가용성 EKS 인프라 구축 프로젝트입니다. 단순 구축을 넘어 **데이터 엔지니어링 관점의 ETL 자동화**와 **인프라 확장성** 확보에 초점을 맞췄습니다.

## 🏗 System Architecture

* **Traffic Control**: ALB (Application Load Balancer)
* **Compute**: Amazon EKS (Application), AWS Lambda (Data Analysis)
* **Database**: Amazon RDS (MySQL)
* **Storage**: Amazon S3 (Data Lake)
* **DevOps/Automation**: EventBridge, ECR, CloudFormation
* **Observability**: Prometheus & Grafana

---

## 🛠 Tech Stack

| Category | Skills |
| :--- | :--- |
| **Infrastructure** | VPC, ALB, NAT Gateway, EKS, RDS, S3 |
| **Data/Serverless** | Python (Pandas, SQLAlchemy), AWS Lambda, EventBridge |
| **DevOps/Container** | Docker, Kubernetes, ECR |
| **Observability** | Prometheus, Grafana |

---

## 🚀 Key Implementations

### 1. High Availability & Security
* **Multi-AZ Architecture**: 2-Public / 4-Private 서브넷 설계를 통한 가용 영역 간 내결함성 확보.
* **Network Isolation**: NAT Gateway 기반의 아웃바운드 통신 제어 및 계층별 보안 그룹(Security Group) 적용.

### 2. Automated Data Pipeline (ETL)
* **Event-Driven**: EventBridge 스케줄러 기반의 Daily Batch 분석 자동화 구현.
* **Serverless Analytics**: Lambda를 활용해 RDS 원천 데이터를 성별/연령별/시간대별 통계로 가공 후 S3(JSON) 적재.
* **Reliability**: 0 나눗셈 예외 처리 및 환경 변수 기반 설정으로 코드 안정성 및 유연성 확보.

### 3. Container Modernization (In Progress)
* **Lambda Dockerization**: 종속성 관리 및 환경 일관성을 위해 Lambda를 컨테이너 이미지(ECR) 방식으로 전환.
* **EKS Integration**: 애플리케이션의 유연한 확장을 위해 EKS 클러스터 도입 및 Pod 배포 진행 중.

---

## 📍 Roadmap & Progress

- [x] **Phase 1: Infrastructure Foundation**
    - VPC/Subnet/SG 네트워크 토폴로지 구성 및 RDS 스키마 설계
- [x] **Phase 2: Serverless Pipeline**
    - Python 기반 데이터 가공 로직 구현 및 Lambda-EventBridge 연동 완료
- [ ] **Phase 3: Containerization & EKS (Current)**
    - [x] 분석 모듈 Docker Image 빌드 및 ECR Push 완료
    - [ ] EKS Cluster 프로비저닝 및 Ingress(ALB) 설정
- [ ] **Phase 4: Full Observability**
    - Prometheus 메트릭 수집 및 Grafana 대시보드 시각화

---

## 💡 Engineering Challenges & Lessons
* **Lambda 의존성 문제**: 분석 라이브러리(Pandas 등) 용량 문제와 로컬/배포 환경 불일치를 해결하기 위해 **Docker 기반 Lambda 배포** 방식을 채택함.
* **데이터 비용 최적화**: 분석 결과를 RDS에 재기록하는 대신 **S3 데이터 레이크**에 저장함으로써 DB 부하를 줄이고 데이터 히스토리 보존성을 높임.
