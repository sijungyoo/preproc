# File Preprocessor 사용자 매뉴얼 (한글)

이 문서는 `app.py` 기반 **File Preprocessor (PySide6/Qt)** 데스크톱 앱의 최종 사용자를 위한 사용법 안내서입니다.

---

## 1) 앱 개요

이 앱은 측정 파일(`xls`, `nasca`, `csv`)을 불러와 다음 작업을 자동화합니다.

- `TimeOutput` 기준으로 curve(subset) 분리
- 각 subset에서 전압/전류 기반 파라미터(`vth`, `vth_2`, `ss`) 계산
- Measure Type에 따라 라벨 컬럼 부여
- 필요한 컬럼만 정리(`voltage`, `current` 등)
- 각 subset 최대 100포인트로 downsample
- 최종 CSV 저장 (`*_processed.csv`)

---

## 2) 실행 방법

### 요구사항
- Python 3.10+
- `requirements.txt`에 있는 의존성 설치 (`PySide6` 포함)
- `nasca` 사용 시: Windows + Excel + `xlwings` 환경 필요

### 실행
```bash
python app.py
```

앱이 실행되면 메인 화면에서 순서대로 설정 후 `Process` 버튼으로 처리합니다.

---

## 3) 메인 화면 구성

앱 UI는 5개 구역으로 구성됩니다.

1. **파일 선택**
2. **출력 폴더**
3. **처리 파라미터**
4. **Measure Type**
5. **실행/진행률/로그**

---

## 4) 기능별 상세 사용법

## 4-1. 파일 선택

- `File type`에서 입력 파일 종류 선택
  - `xls`
  - `nasca` (`xls` 기반이지만 DRM 파일 로딩 방식 다름)
  - `csv`
- `파일 선택 시작 경로`를 먼저 지정할 수 있습니다.
  - `찾기`: 파일 선택 창이 처음 열릴 경로(초기 경로) 지정
- `파일 선택(다중)` 버튼으로 여러 파일 선택 가능
  - 파일 선택 창은 지정한 초기 경로에서 시작합니다.
- 목록에는 **파일명만** 표시됩니다 (경로는 내부적으로 유지)
- 목록에서 체크된(선택된) 파일만 실제 처리 대상입니다.
- 초기 경로는 프로그램 종료 후에도 저장되어 다음 실행 시 유지됩니다.

> 팁: 파일을 선택하면 기본적으로 전체 항목이 자동 선택됩니다.

---

## 4-2. 출력 폴더

- `2. 출력 폴더`에서 저장 경로 지정
- 기본값 예시: `D:\Multimedia\upload\output_YYMMDD`
- 처리 결과는 입력 파일당 `<원본파일명>_processed.csv`로 저장됩니다.

---

## 4-3. 처리 파라미터

### 전압 컬럼명 / 전류 컬럼명
- 원본 데이터에서 사용할 컬럼명을 지정합니다.
- 기본값:
  - 전압: `VMeasCh2`
  - 전류: `ID`

### Vth 임계전류값
- `vth`/`vth_2` 계산 기준 전류 임계값
- 예: `1e-7`

### Curve 분리 최소 간격
- `TimeOutput` 차이가 이 값 이상이면 새로운 subset으로 분리
- 예: `1e-5`

> 주의: `TimeOutput` 컬럼이 없으면 처리 오류가 발생합니다.

---

## 4-4. Measure Type

`ISPP`, `Endurance`, `Retention`, `Custom` 중 선택합니다.

### A. ISPP / Endurance / Retention

`Measure 설정` 버튼으로 상세 설정합니다.

설정 항목:
- **Target Parameter Name(s)**: Sheet3 metadata에서 읽을 key 이름들(콤마 구분)
- **Subset Label Column Header**: 출력 라벨 컬럼명
- **Append Condition Parameter Name(s)**: 추가로 붙일 metadata key들
- **Polarity**: `PGM`, `ERS`, `PGM/ERS`, `ERS/PGM`

#### 라벨 계산 규칙 요약

- **ISPP**: `V_min`, `V_max`, `V_step`으로 등간격 생성
  - `V_step`은 양/음수 모두 가능
  - 단, `V_min → V_max` 방향과 부호가 맞아야 함
  - `V_step == 0`은 허용되지 않음
- **Retention**: `Retention_min`부터 10배씩 증가, 마지막 `Retention_max` 포함
- **Endurance**: `0, 1, 10, 100, ...` 형태 + 최종 `Cycle` 포함

#### Polarity 동작

- `PGM`, `ERS`: 각 label당 동일 polarity 1개
- `PGM/ERS`, `ERS/PGM`: 각 label당 2개 subset 필요

> subset 개수와 label×polarity 개수가 맞지 않으면 오류가 납니다.

### B. Custom

`Custom Label 설정` 버튼으로 사용자 라벨을 직접 입력합니다.

#### 스프레드시트(QTableWidget) 기반 입력 UI 사용법
1. `라벨 컬럼 수` 입력 후 `적용`
2. 상단에서 컬럼명 수정 (예: `state`, `batch`, `temp`)
3. 표에서 subset별 셀을 직접 편집 (더블클릭/키보드 입력)
4. 범위 선택 후 **Ctrl+C / Ctrl+V**로 행·열 단위 복사/붙여넣기
5. `확인`으로 저장

검증 규칙:
- 컬럼명 공백 불가
- 컬럼명 중복 불가

---

## 4-5. 실행 및 진행 상태

- `Process` 클릭 시 백그라운드 스레드로 처리 시작
- 진행바 + 상태 텍스트 업데이트
- 하단 로그 창에 파일별 처리 결과/오류 출력
- 완료 시 저장 파일 개수 메시지 표시

---

## 5) 출력 CSV 구조

입력 형식과 Measure Type에 따라 달라질 수 있으나, 일반적으로 다음 컬럼이 포함됩니다.

- `voltage`
- `current`
- `vth`, `vth_2`, `ss`
- Measure 라벨 컬럼 (예: `write_V`, `retention`, `cycle` 등)
- `polarity`
- 조건 파라미터 컬럼들(설정 시)

> Measure 라벨 헤더는 첫 글자가 소문자로 정규화될 수 있습니다.

---

## 6) 자주 발생하는 오류와 해결

### 1) "Column 'TimeOutput' not found"
- 원본 파일에 `TimeOutput` 컬럼이 있는지 확인
- 헤더 철자/공백 확인

### 2) "Voltage/Current column not found"
- `전압 컬럼명`, `전류 컬럼명` 입력값이 실제 컬럼명과 일치하는지 확인

### 3) ISPP 관련 오류
- `V_step은 0일 수 없습니다.` → step 값을 0이 아닌 값으로 수정
- `V_min→V_max 방향과 V_step 부호가 일치해야 합니다.`
  - 증가 sweep: `v_min < v_max`, `v_step > 0`
  - 감소 sweep: `v_min > v_max`, `v_step < 0`

### 4) subset 개수 불일치
- metadata 기반 label 개수와 실제 subset 개수 확인
- 특히 polarity가 `PGM/ERS`, `ERS/PGM`이면 subset 수가 2배 필요

### 5) nasca 로딩 실패
- Excel 설치/실행 가능 상태인지 확인
- 파일 접근 권한/잠금 상태 확인

---

## 7) 권장 운영 팁

- 파일 처리 전 샘플 1개로 먼저 테스트
- Custom 라벨은 컬럼명 규칙을 팀 내에서 표준화
- 대량 처리 시 출력 폴더를 날짜별로 분리
- 로그를 보면서 오류 파일만 재처리

---

## 8) 빠른 시작(Quick Start)

1. `python app.py` 실행
2. `File type` 선택 + 파일 다중 선택
3. 필요 시 `파일 선택 시작 경로`를 설정
4. `파일 선택(다중)`으로 대상 파일 선택
5. 출력 폴더 지정
6. 컬럼명/임계값 확인
7. Measure Type 선택
   - ISPP/Retention/Endurance: `Measure 설정`
   - Custom: `Custom Label 설정`
8. `Process` 클릭
9. 로그 확인 + `*_processed.csv` 결과 점검

---

## 9) 스크린샷 안내

현재 문서 작성 환경에서는 GUI를 직접 실행해 캡처 이미지를 자동 생성할 수 없어 스크린샷을 포함하지 못했습니다.
실사용 환경에서 아래 화면을 캡처해 이 문서에 추가하면 사용자 이해도가 크게 높아집니다.

권장 캡처 목록:
- 메인 화면 전체
- Measure 설정 팝업
- Custom Label(스프레드시트) 팝업
- 처리 완료 후 로그/결과 예시
