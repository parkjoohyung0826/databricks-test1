## 변경 요약

<!-- 무엇을 왜 변경했는지 2~5문장으로 작성해 주세요. -->

## 관련 이슈

<!-- 예: Closes #123 또는 Relates to #123 -->

Closes #

## 변경 영역

- [ ] Bronze / 원천 적재
- [ ] Silver / 정제
- [ ] Gold / 마트
- [ ] Job / Workflow
- [ ] Pipeline
- [ ] Dashboard
- [ ] ML / Model
- [ ] Bundle / 배포 설정
- [ ] CI/CD
- [ ] 문서 / 테스트

## 환경 및 데이터 영향

- 대상 환경: <!-- dev / prod / 모두 -->
- 대상 catalog.schema: <!-- 예: wonik_dev.wonik_test1_bronze -->
- [ ] 데이터 읽기만 수행합니다.
- [ ] 테이블 또는 데이터를 생성·변경합니다.
- [ ] 스키마를 변경합니다.
- [ ] 삭제, 교체, overwrite 등 파괴적 변경이 있습니다.
- [ ] Job 스케줄, Dashboard URL 또는 리소스 ID가 변경될 수 있습니다.

<!-- 데이터 영향이나 마이그레이션 방법을 구체적으로 작성해 주세요. -->

## 검증 결과

- [ ] `databricks bundle validate --target dev`
- [ ] 개인 dev target 배포
- [ ] 변경한 노트북 또는 Job 실행
- [ ] 출력 테이블·행 수·스키마 확인
- [ ] Dashboard 쿼리 및 시각화 확인
- [ ] prod target 검증

검증 근거:

<!-- 실행 URL, Job run ID, 쿼리 결과, 스크린샷 등을 첨부해 주세요. -->

## 배포 및 롤백

- 배포 방법: <!-- 예: main 머지 후 production Environment 승인 -->
- 롤백 방법: <!-- 이전 커밋 재배포, 테이블 복원 방법 등 -->

## 최종 체크리스트

- [ ] `wonik_poc` 하드코딩 여부를 확인했습니다.
- [ ] dev 코드가 prod 데이터에 쓰지 않는지 확인했습니다.
- [ ] Secret, 토큰, 개인정보를 커밋하지 않았습니다.
- [ ] 다른 개발자의 dev 리소스 또는 공용 테이블과 충돌하지 않습니다.
- [ ] 파괴적 변경과 운영 영향이 PR에 명시되어 있습니다.

