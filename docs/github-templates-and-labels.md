# GitHub PR, Issue, Label 운영

## 템플릿 활성화

GitHub는 다음 파일이 기본 브랜치에 머지되면 자동으로 인식합니다.

- `.github/pull_request_template.md`: 새 PR 본문에 자동 적용
- `.github/ISSUE_TEMPLATE/work-item.yml`: New issue 화면에 `Databricks 작업 등록` 양식 표시
- `.github/ISSUE_TEMPLATE/config.yml`: 빈 이슈 생성 제한과 참고 링크 설정

따라서 이 변경을 feature 브랜치에 commit/push하고 PR로 `main`에 머지하면 별도 설치 없이 템플릿이 활성화됩니다.

## 라벨 등록

`.github/labels.yml`은 이 저장소의 라벨 표준 원본이지만 GitHub가 자동으로 라벨을 생성하지는 않습니다. 라벨은 GitHub UI 또는 GitHub CLI로 한 번 등록해야 합니다.

### GitHub UI

1. 저장소의 `Issues`를 엽니다.
2. `Labels`를 선택합니다.
3. `New label`을 클릭합니다.
4. `.github/labels.yml`의 `name`, `description`, `color`를 입력합니다.

최소한 `status:triage`를 먼저 생성해야 Issue Form의 기본 라벨이 정상 적용됩니다.

### GitHub CLI 예시

GitHub CLI 인증 후 다음 형식으로 생성합니다.

```powershell
gh auth login
gh label create "status:triage" --color "EDEDED" --description "범위와 담당자 확인 필요"
gh label create "type:feature" --color "1D76DB" --description "새로운 기능 또는 데이터 제품 개발"
gh label create "area:bronze" --color "CD7F32" --description "Bronze 원천 적재 및 테이블"
gh label create "env:dev" --color "C2E0C6" --description "개인 또는 공용 개발 환경 대상"
gh label create "risk:destructive" --color "B60205" --description "DROP, DELETE, OVERWRITE, 재생성 등 파괴적 변경"
```

동명의 라벨을 갱신하려면 `--force`를 추가합니다.

```powershell
gh label create "env:prod" --color "B60205" --description "운영 환경 대상" --force
```

## 권장 사용 방식

1. 작업 시작 전에 Issue Form으로 작업을 등록합니다.
2. Issue에 type, area, env, risk 라벨을 붙입니다.
3. `feature/<issue-number>-<short-name>` 브랜치를 만듭니다.
4. 개인 dev target에서 실행하고 검증합니다.
5. PR 본문에 `Closes #<issue-number>`를 작성합니다.
6. 데이터 영향, 검증 근거와 롤백 방법을 채웁니다.
7. PR 검증과 리뷰가 완료되면 `main`에 머지합니다.
8. production Environment 승인 후 prod를 배포합니다.

권장 라벨 조합 예시:

```text
type:feature + area:silver + env:dev + risk:data-write
type:bug + area:dashboard + env:prod + priority:high
type:refactor + area:bundle + area:ci-cd
```
